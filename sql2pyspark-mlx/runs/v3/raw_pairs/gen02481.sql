select store_id, sum(shipping_fee) as total_shipping_fee, rank() over (order by sum(shipping_fee) desc) as shipping_fee_rank
from orders
group by store_id
order by store_id desc;
