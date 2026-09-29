select s.id, s.city, s.sqft from stores s where s.sqft = (select max(s2.sqft) from stores s2 where s2.city = s.city) order by s.id desc
