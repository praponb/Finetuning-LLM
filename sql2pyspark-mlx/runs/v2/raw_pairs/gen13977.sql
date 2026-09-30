select id, customer_id, order_date, discount, lead(discount, 1) over (partition by customer_id order by order_date, id) as next_discount from orders where store_id is null;
