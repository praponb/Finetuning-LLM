select
    i.id,
    i.store_id,
    i.quantity,
    p.category,
    p.launch_date,
    s.city
from inventory i
left join products p on i.product_id = p.id
inner join stores s on s.id = i.store_id
where i.quantity != 94 and s.region = 'Central';
