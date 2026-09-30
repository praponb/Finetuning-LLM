select s.id, s.country, s.rating from suppliers s where s.rating = (select max(s2.rating) from suppliers s2 where s2.country = s.country);
