select
    oi.id,
    oi.product_id,
    oi.quantity,
    o.store_id,
    o.order_date,
    o.status
from order_items oi
inner join orders o on oi.order_id = o.id
where o.amount between 1240 and 1290 and oi.product_id in (1, 7)
order by 2 desc, id
limit 20
