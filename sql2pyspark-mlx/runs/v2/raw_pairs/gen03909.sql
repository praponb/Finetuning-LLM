select
    i.id,
    i.product_id,
    i.store_id,
    p.cost,
    p.category,
    s.country,
    s.city
from inventory i
inner join products p on i.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
where s.city not in ('Bangkok', 'New York')
order by store_id desc, id
limit 10;
