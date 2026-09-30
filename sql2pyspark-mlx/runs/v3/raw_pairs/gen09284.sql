select id, order_date, discount, lag(discount, 1) over (order by order_date, id) as prev_discount from orders where customer_id is not null order by 2 desc
