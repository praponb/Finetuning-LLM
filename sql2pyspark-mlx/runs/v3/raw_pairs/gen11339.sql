select s.id, s.rating, s.supplier_name
from suppliers s
where not exists (select 1 from products p where p.supplier_id = s.id)
order by id, s.supplier_name
limit 10;
