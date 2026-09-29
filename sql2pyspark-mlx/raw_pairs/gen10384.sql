select
    s.id,
    s.country,
    s.city,
    s.supplier_name
from suppliers s
where s.rating >= 2.1 and not exists (select 1 from products p where p.supplier_id = s.id and p.brand = 'Stark')
order by 4
