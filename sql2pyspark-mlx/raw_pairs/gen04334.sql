select s.id, s.country, s.rating, s.supplier_name from suppliers s where s.rating = (select min(s2.rating) from suppliers s2 where s2.country = s.country) order by s.country;
