select s.id, s.city, s.sqft, s.region from stores s where s.sqft = (select min(s2.sqft) from stores s2 where s2.city = s.city) order by s.city desc
