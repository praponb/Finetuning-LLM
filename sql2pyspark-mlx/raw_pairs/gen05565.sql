select
    oi.order_id,
    p.supplier_id,
    p.product_name,
    o.channel,
    o.status
from order_items oi
left join products p on oi.product_id = p.id
inner join orders o on oi.order_id = o.id
where o.order_date > '2025-10-06'
order by oi.order_id asc;
