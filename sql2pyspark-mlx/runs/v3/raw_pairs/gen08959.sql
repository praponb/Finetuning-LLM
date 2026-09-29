select * from (select channel, status, discount from orders where customer_id is null)
pivot (max(discount) for status in ('CANCELLED', 'PENDING', 'SHIPPED'))
order by channel
