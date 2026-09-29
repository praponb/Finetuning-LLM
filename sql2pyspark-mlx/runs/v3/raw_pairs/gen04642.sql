select date_format(order_date, 'yyyy-MM') as order_month, channel, min(discount) as min_discount
from orders
where shipping_fee <= 17 and channel in ('partner', 'store', 'web')
group by rollup(date_format(order_date, 'yyyy-MM'), channel);
