select s.id, s.country from suppliers s where not exists (select 1 from products p where p.supplier_id = s.id and p.price < 360) order by s.id desc
