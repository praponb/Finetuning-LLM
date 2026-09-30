select
    o.status,
    o.customer_id,
    sum(oi.unit_price * oi.quantity) as revenue,
    min(oi.unit_price) as min_unit_price
from order_items oi
left join orders o on oi.order_id = o.id
where o.shipping_fee > 23
group by o.status, o.customer_id
order by min_unit_price desc
