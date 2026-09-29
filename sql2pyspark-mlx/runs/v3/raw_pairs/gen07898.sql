select s.id, s.city, s.sqft, s.store_name from stores s where s.sqft > (select avg(s2.sqft) from stores s2 where s2.city = s.city) order by s.sqft desc;
