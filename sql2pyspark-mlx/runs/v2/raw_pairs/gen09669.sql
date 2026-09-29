select year(order_date) as order_year, channel, count(shipping_fee) as shipping_fee_count
from orders
where status in ('PENDING', 'CANCELLED', 'SHIPPED')
group by year(order_date), channel;
