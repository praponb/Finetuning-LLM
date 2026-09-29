select id, country, started_at, dense_rank() over (partition by country order by started_at desc) as started_at_dense_rank from sessions where customer_id = 15;
