select
    oi.order_id,
    oi.unit_price,
    o.status,
    p.supplier_id
from order_items oi
inner join orders o on oi.order_id = o.id
right join products p on oi.product_id = p.id
where array_contains(p.tags, 'clearance');
