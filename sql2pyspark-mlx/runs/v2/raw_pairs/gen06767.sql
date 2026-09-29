select id, customer_id, started_at, lag(started_at, 1) over (partition by customer_id order by started_at, id) as prev_started_at from sessions;
