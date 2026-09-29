with Ranked as (select id, customer_id, started_at, row_number() over (partition by customer_id order by started_at, id) rnk from sessions) select * from Ranked where rnk <= 5;
