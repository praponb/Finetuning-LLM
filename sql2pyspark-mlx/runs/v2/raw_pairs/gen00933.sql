with ranked as (select id, customer_id, order_date, amount, rank() over (partition by customer_id order by order_date) as pos from orders) select * from ranked where pos <= 1
