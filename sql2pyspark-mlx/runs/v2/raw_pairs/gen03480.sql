select s.id, s.rating, s.country from suppliers s where s.id in (select supplier_id from products)
