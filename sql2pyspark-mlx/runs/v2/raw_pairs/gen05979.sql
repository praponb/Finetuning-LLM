select s.id, s.rating, s.city, s.country from suppliers s where exists (select 1 from products p where p.supplier_id = s.id and p.category in ('Electronics', 'Sports', 'Books')) order by city
