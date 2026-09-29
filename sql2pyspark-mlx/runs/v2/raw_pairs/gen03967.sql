select channel, min(order_date) as min_order_date, sum(case when status = 'RETURNED' then 1 else 0 end) as returned_count
from orders
where shipping_fee >= 27 and (channel = 'mobile' or customer_id is null)
group by channel
having sum(case when status = 'RETURNED' then 1 else 0 end) >= 50
