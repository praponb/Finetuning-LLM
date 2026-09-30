select
    p.cost,
    p.supplier_id,
    s.supplier_name,
    s.country,
    s.city
from products p
join suppliers s on p.supplier_id = s.id
where s.city in ('Chiang Mai', 'Bangkok', 'London')
order by p.cost desc
