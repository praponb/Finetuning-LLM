select s.id, s.rating from suppliers s where s.rating between 2.4 and 3.2 and exists (select 1 from products p where p.supplier_id = s.id and p.price < 550) order by s.id
