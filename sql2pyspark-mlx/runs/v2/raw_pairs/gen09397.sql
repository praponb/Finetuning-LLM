select date_trunc('MONTH', o.order_date) as order_month, round(stddev(oi.unit_price), 2) as stddev_unit_price, count(*) as num_order_items
from order_items oi
left outer join orders o on oi.order_id = o.id
group by date_trunc('MONTH', o.order_date)
order by stddev_unit_price, order_month
