select id, customer_id, started_at, dense_rank() over (partition by customer_id order by started_at desc) as started_at_dense_rank from sessions order by started_at;
