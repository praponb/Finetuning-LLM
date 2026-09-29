select
    oi.order_id,
    oi.product_id,
    p.cost,
    s.city
from order_items oi
right outer join products p on p.id = oi.product_id
inner join suppliers s on p.supplier_id = s.id
where p.launch_date <= '2023-05-24' and array_contains(p.tags, 'premium')
