select
    oi.id,
    oi.quantity,
    p.category,
    p.supplier_id,
    o.amount,
    floor(oi.quantity) as quantity_whole
from order_items oi
left outer join products p on p.id = oi.product_id
right outer join orders o on oi.order_id = o.id
where o.shipping_fee >= 23 or o.order_date >= '2024-05-01'
order by category, oi.quantity;
