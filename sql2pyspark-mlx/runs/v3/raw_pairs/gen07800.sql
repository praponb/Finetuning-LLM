select customer_id, sum(shipping_fee) as total_shipping_fee, sum(case when channel = 'partner' then amount else 0 end) as partner_amount
from orders
group by customer_id
having sum(shipping_fee) > 130
order by total_shipping_fee desc, customer_id desc;
