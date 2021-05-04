import time
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import json
import requests

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",6100)

lines = dataStream.map(lambda x:x.split('\n'))

records = lines.map(lambda x: json.loads(x[0]))

def func_eve(x):
	if len(x)==12:
		return x

events = records.filter(func_eve)

def func_match(x):
	if len(x)!=12:
		return x

match = records.filter(func_match)

def own_go(x):
	for i in x['tags']:
		if i==102:
			return (x['playerId'],1)
	return (x['playerId'], 0)
			
def own_foul(x):
	if x['eventId']==2:
		return (x['playerId'],1)
	else:
		return (x['playerId'],0)


own = events.map(own_go)
own_goal = own.reduceByKey(lambda x,y:x+y)
#own_goal.pprint()

foul = events.map(own_foul)
foulss = foul.reduceByKey(lambda x,y:x+y)
#foulss.pprint()


foul_go = foulss.join(own_goal)
#foul_go.pprint()

#duel effectiveness
def get_neutral(x):
    for i in x['tags']:
        if i['id'] == 702:
            return (x['playerId'],1)
        return (x['playerId'],0)
def get_won(x):
    for i in x['tags']:
        if i['id'] == 703:
            return (x['playerId'],1)
        return (x['playerId'],0)

events=records.filter(lambda x: len(x) == 12)#FILTERS the event records

trial = events.filter(lambda x: x['eventId'] ==10)#filters records with eventID 10 

trial1 = events.filter(lambda x: x['eventId'] ==1)#filters records with eventID 1

passes = events.filter(lambda x: x['eventId'] ==8)#filters records with eventID 8, ie. passes

#-------#

import csv


def func_play_id(x):
	f = open('test.csv', 'w')
	play = list()
	dat = x['teamsData']
	teamid = list(dat.keys())
	
	for team in teamid:
		form = dat[team]['formation']
		for i in form['bench']:
			li = dict()
			li["playerId"] = i['playerId']
			li["contri"]=0
			li["teamId"] = team
			li["goals"] = i['goals']
			
			#for key in li.keys():
			#	f.write("%s,"%(li[key]))
			#f.write("\n")
			play.append(li)
		for i in form['lineup']:
			li = dict()
			li["playerId"]= i['playerId']
			li["contri"]= 1.05
			li["teamId"] = team
			li["goals"] = i['goals']
			play.append(li)
		for i in form['substitutions']:
			for j in play:
				if i["playerIn"] == j["playerId"]:
					j["contri"] = (90-i['minute'])/90
				if i['playerOut'] == j["playerId"]:
					j["contri"]=(i['minute'])/90
	for li in play:
		for key in li.keys():
				f.write("%s,"%(li[key]))
		f.write("\n")
	f.close()
	return play

play_id = match.map(func_play_id)
#play_id.pprint()

#-------#

#PASS ACCURACY
def get_accpass(x):
	for i in x['tags']:
		if i['id'] == 1801:
			return 1
	return 0
			
def get_inaccpass(x):
	for i in x['tags']:
		if i['id'] == 1802:
			return 1
	return 0
			
def get_keypass(x):
	for i in x['tags']:
		if i['id'] == 302:
			return 1
	return 0
						
def get_keypasses(x):
	for i in x['tags']:
		if i['id'] == 302:
			return (x['playerId'],1)
	return (x['playerId'],0)
			
def get_notkeypass(x):
	for i in x['tags']:
		if i['id'] == 302:
			return (x['playerId'],0)
	return (x['playerId'],1)

def get_acc_keypass(x):
	if get_keypass(x) and get_accpass(x):
		return (x['playerId'],1)
	else:
		return (x['playerId'],0)

def get_acc_notkeypass(x):
	if get_accpass(x):
		for i in x['tags']:
			if i['id'] == 302:
				return (x['playerId'],0)
		return (x['playerId'],1)
	else:
		return (x['playerId'],0)


#PASS ACCURACY
acc_normalpass = passes.map(get_acc_notkeypass)
acc_keypass = passes.map(get_acc_keypass)
all_normalpass = passes.map(get_notkeypass)
all_keypass = passes.map(get_keypasses)

#acc_normalpass_count=acc_normalpass.map(lambda x: (x['playerId'],1))
total_acc_normalpass=acc_normalpass.reduceByKey(lambda a,b:a +b)

#acc_keypass_count=acc_keypass.map(lambda x: (x['playerId'],1))
total_acc_keypass=acc_keypass.reduceByKey(lambda a,b:a +b)

num_join=total_acc_normalpass.join(total_acc_keypass)

pass_acc_num = num_join.map(lambda x: (x[0],x[1][0] + 2*x[1][1]))

#normalpass_count=all_normalpass.map(lambda x: (x['playerId'],1))
total_normalpass=all_normalpass.reduceByKey(lambda a,b:a +b)

#keypass_count=all_keypass.map(lambda x: (x['playerId'],1))
total_keypass=all_keypass.reduceByKey(lambda a,b:a +b)

den_join = total_normalpass.join(total_keypass)
pass_acc_den = den_join.map(lambda x: (x[0],x[1][0] + 2*x[1][1]))

final_passjoin=pass_acc_num.join(pass_acc_den)

pass_accuracy = final_passjoin.map(lambda x:(x[0], round((x[1][0]/x[1][1]),5)))

#-------#



#DUEL EFFECTIVENESS
neutral_duel = trial1.map(get_neutral)
won_duel= trial1.map(get_won)
#won_duel.pprint()
total_neutral=neutral_duel.reduceByKey(lambda a,b:a +b)

total_won=won_duel.reduceByKey(lambda a,b:a +b)

dj_1=total_won.join(total_neutral)
#dj_1.pprint() #gives something like this (7918, (5, 5)) (player,won,neutral)
a1=dj_1.map(lambda x: (x[0],x[1][0] + 0.5 *x[1][1]))
#a1.pprint()  # (3560, 6.5)
#count of duels
m= trial1.map(lambda x: (x['playerId'],1))
total_duels=m.reduceByKey(lambda a,b:a +b)
#total_duels.pprint() #(15054, 19)
a2=a1.join(total_duels)
#a2.pprint()  #(20450, (5.0, 19))
duel_eff=a2.map(lambda x:(x[0], round((x[1][0]/x[1][1]),5)))
#duel_eff.pprint() # (25804, 0.28846)


#-------#


#shot effectiveness
def get_target(x):
	for i in x['tags']:
		if i['id'] == 1801:
			return x

def get_target1(x):
		for i in x['tags']:
			if i['id'] == 1801:
				return (x['playerId'],1)
		return (x['playerId'],0)

def get_nottarget(x):
	for i in x['tags']:
		if i['id'] == 1802:
			return (x['playerId'],1)
	return (x['playerId'], 0)

def goals(x):
	for i in x['tags']:
		if i['id'] == 101:
			return (x['playerId'],1)
	return (x['playerId'],0)


#SHOT EFFECTIVENESS
shots_on_target = trial.map(get_target1)#gets shots on target
total_targets=shots_on_target.reduceByKey(lambda a,b:a +b)#gets count of shots on target. total_targets = (eventId, count)

shots_non_target = trial.map(get_nottarget)#gets shots not on target

shots_on_targ = trial.filter(get_target)
shots_and_goals = shots_on_targ.map(goals)#gets goals from records which have shots on targets

#shots_on_target_count=shots_on_target.map(lambda x: (x['playerId'],1))
#shots_and_goals_count = shots_and_goals.map(lambda x: (x['playerId'],1))

total_goals=shots_and_goals.reduceByKey(lambda a,b:a +b)#gets count of goals . total_goal = (eventId, here 10, count of goals)

shots_non_goal = total_targets.join(total_goals)#DStream of the form (eventId, (count of total_targets, count of total_goals))
shots_non_goals_count=shots_non_goal.map(lambda x: (x[0],x[1][0] - x[1][1]))#gets total on target not goals records

joined_1= total_goals.join(shots_non_goals_count)# joining (eventID, (total goals, shots on target and not goals count))
shots_numerator=joined_1.map(lambda x: (x[0],x[1][0] + 0.5 *x[1][1]))

#count of shots
x = trial.map(lambda x: (x['playerId'],1))
total_shots=x.reduceByKey(lambda a,b:a +b) #total_shots count

joined_2= shots_numerator.join(total_shots)#joining numerator and total shots
shot_effectiveness=joined_2.map(lambda x:(x[0], round((x[1][0]/x[1][1]),5)))#gets the shot effectiveness.
#shot_effectiveness.pprint()
#new=total_shots.map(lambda x: x[1])


#-------#



# free kick effectiveness

def func_3(x):
	if (len(x)==12):
		if (x['eventId']==3):
			return x


def func_1802(x):
		for i in x['tags']:
			if i['id'] == 1802:
				return (x['playerId'],1)
		return (x['playerId'], 0)

def func_1801(x):
		for i in x['tags']:
			if i['id'] == 1801:
				return (x['playerId'],1)
		return (x['playerId'],0)


def func_1801_2(x):
		for i in x['tags']:
			if i['id'] == 1801:
				return x

def func_pengoal(x):
	if x['subEventId']==35:
			for i in x['tags']:
				if i['id']==101:
					return (x['playerId'],1)
	else:
		return (x['playerId'], 0)

free_kick = records.filter(func_3)

#Inaccurate passes
a_ = free_kick.map(func_1802)
a_inter1 = a_.reduceByKey(lambda x,y: x+y)

#accurate passes
b_ = free_kick.map(func_1801)
b_inter1 = b_.reduceByKey(lambda x,y: x+y)


# total free kick = accurate + inaccurate passes
a_b_inter = a_inter1.join(b_inter1)
a_b = a_b_inter.map(lambda x:(x[0], x[1][0] + x[1][1]))

# Penalties which are goal
b2 = free_kick.filter(func_1801_2)
b_1 = b2.map(func_pengoal)
b_1_inter1 = b_1.reduceByKey(lambda x,y: x+y)


# number of penalties + number of effective free kicks
b_b_1 = b_inter1.join(b_1_inter1)
b_b_1_fin = b_b_1.map(lambda x:(x[0], x[1][0] + x[1][1]))


#division
free_effect_inter = b_b_1_fin.join(a_b)
free_effect = free_effect_inter.map(lambda x:(x[0], round((x[1][0] / x[1][1]),5)))


#-------#



pass_duel=pass_accuracy.join(duel_eff)
#pass_duel.pprint()

pass_duel1=pass_duel.map(lambda x:(x[0], round((x[1][0] + x[1][1]),5)))


pass_duel_shot = pass_duel1.join(shot_effectiveness)
pass_duel_shot1=pass_duel_shot.map(lambda x:(x[0], round((x[1][0] + x[1][1]),5)))

pass_duel_shot_free = pass_duel_shot1.join(free_effect)
contribution = pass_duel_shot.map(lambda x:(x[0], round((x[1][0] + x[1][1])/4,5)))

#-------#

def normalizerize(x):
	f = open("test.csv", "r")
	reader=csv.reader(f)
	for row in reader:
		if int(row[0]) == x[0]:
			return tuple(x[0], int(row[1])*x[1])
		
	f.close()
	return x
	
r=contribution.map(normalizerize)	
r.pprint()
#-------#
#time.sleep(2)

#PLAYER PERFORMANCE
res = foul_go.join(contribution) # (49876, ((1, 0), 0.23824))
#res[0] playerid
#res[1][0][0] fouls
#res[1][0][1] own goals
#res[1][1]= contri
res1=res.map(lambda x: (x[0],x[1][1]-(((x[1][0][1]*0.005)+(x[1][0][1]*0.05))*x[1][1])))
#res1.pprint()

#-------#



ssc.start()
ssc.awaitTermination()
ssc.stop()
