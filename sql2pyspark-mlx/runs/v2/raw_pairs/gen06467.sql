select s.id, s.rating, s.city
from suppliers s
where not exists (select 1 from products p where p.supplier_id = s.id and p.product_name like '%Sma%');
