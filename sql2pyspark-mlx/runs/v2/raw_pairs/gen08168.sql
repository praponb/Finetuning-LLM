select s.id, s.rating
from suppliers s
where not exists (select 1 from products p where p.supplier_id = s.id and p.supplier_id = 23)
order by rating desc
