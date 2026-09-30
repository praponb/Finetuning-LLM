select p.id, p.brand, s.country, s.rating from products p join suppliers s on p.supplier_id = s.id order by brand, id limit 3
