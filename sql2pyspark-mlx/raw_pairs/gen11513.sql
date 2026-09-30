select id, customer_id, amount, dense_rank() over (partition by customer_id order by amount desc) as dense_rnk from orders order by id
