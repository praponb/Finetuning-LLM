select channel, sum(shipping_fee) as total_shipping_fee, sum(sum(shipping_fee)) over (order by channel) as cumulative_shipping_fee
from orders
where store_id = 9
group by channel
order by channel, total_shipping_fee desc
