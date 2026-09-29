select id, started_at, row_number() over (order by started_at desc, id) as rn from sessions where customer_id = 7;
