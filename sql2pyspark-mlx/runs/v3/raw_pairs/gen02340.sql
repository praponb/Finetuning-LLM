select id, customer_id, opened_date, dense_rank() over (partition by customer_id order by opened_date desc) opened_date_dense_rank from accounts
