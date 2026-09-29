select id, customer_id, order_date, amount, amount - lag(amount, 1, 0) over (partition by customer_id order by order_date, id) as amount_change from orders where discount >= 0.19;
