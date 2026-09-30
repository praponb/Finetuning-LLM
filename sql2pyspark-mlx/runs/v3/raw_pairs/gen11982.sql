select channel, max(shipping_fee) - min(shipping_fee) as shipping_fee_range
from orders
where status in ('SHIPPED', 'COMPLETED') and (status <> 'COMPLETED' or customer_id in (6, 8, 13, 24))
group by channel
having shipping_fee_range < 63
order by channel
