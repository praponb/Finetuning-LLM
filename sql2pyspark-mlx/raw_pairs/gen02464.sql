select
    s.id,
    s.supplier_name,
    s.rating,
    s.city
from suppliers s
where exists (select 1 from products p where p.supplier_id = s.id and p.product_name like '%wireles%')
