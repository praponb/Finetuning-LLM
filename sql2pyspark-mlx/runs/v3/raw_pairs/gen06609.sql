select a.id, a.city, a.sqft from stores a where a.sqft < (select avg(b.sqft) from stores b where b.city = a.city);
