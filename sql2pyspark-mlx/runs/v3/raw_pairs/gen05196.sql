select o.channel, date_trunc('MONTH', o.order_date) as order_month, max(oi.unit_price) - min(oi.unit_price) as unit_price_range
from order_items oi
inner join orders o on o.id = oi.order_id
inner join customers c on c.id = o.customer_id
group by o.channel, date_trunc('MONTH', o.order_date)
