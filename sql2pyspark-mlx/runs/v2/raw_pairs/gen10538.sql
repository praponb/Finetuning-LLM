select id, customer_id, opened_date, lead(opened_date, 1) over (partition by customer_id order by opened_date, id) as next_opened_date from accounts order by id desc;
