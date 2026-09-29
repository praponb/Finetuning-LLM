select s.id, s.rating
from suppliers s
where not exists (select 1 from products p where p.supplier_id = s.id)
order by s.id, rating desc
