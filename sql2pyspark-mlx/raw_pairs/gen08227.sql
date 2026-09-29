select id, customer_id, order_date, discount, lead(discount, 1) over (partition by customer_id order by order_date, id) next_discount from orders where discount < 0.4 order by discount desc;
