select channel, sum(shipping_fee) as total_shipping_fee, dense_rank() over (order by sum(shipping_fee) desc) as shipping_fee_rank
from orders
where discount between 0.1 and 0.18
group by channel;
