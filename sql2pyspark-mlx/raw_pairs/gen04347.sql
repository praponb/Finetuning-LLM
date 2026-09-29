select
    p.category,
    p.launch_date,
    oi.quantity,
    oi.product_id,
    array_contains(p.tags, 'new') as is_new
from products p
left outer join order_items oi on p.id = oi.product_id
where p.brand = 'Stark'
