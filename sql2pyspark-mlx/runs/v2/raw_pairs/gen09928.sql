select * from (select channel, status, discount from orders)
pivot (sum(discount) for status in ('SHIPPED', 'COMPLETED', 'RETURNED'))
