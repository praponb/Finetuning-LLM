select * from (select date_format(order_date, 'yyyy-MM') as order_month, status, discount from orders)
pivot (count(discount) for status in ('COMPLETED', 'SHIPPED', 'PENDING'))
order by order_month
