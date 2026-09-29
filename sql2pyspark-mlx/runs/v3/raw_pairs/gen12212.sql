select a.id, a.country, a.rating from suppliers a where a.rating = (select min(b.rating) from suppliers b where b.country = a.country) order by a.rating desc
