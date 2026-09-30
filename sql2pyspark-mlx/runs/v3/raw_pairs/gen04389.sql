select
    p.id,
    p.category,
    count(i.id) as inventory_count,
    max(i.quantity) - min(i.quantity) as quantity_range
from products p
inner join inventory i on p.id = i.product_id
where p.brand = 'Stark'
group by p.id, p.category
order by quantity_range desc, id asc, category asc
limit 5;
