select a.id, a.country, a.rating, a.supplier_name from suppliers a where a.rating > (select avg(b.rating) from suppliers b where b.country = a.country);
