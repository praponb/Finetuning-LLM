select id, customer_id, start_date, row_number() over (partition by customer_id order by start_date desc, id) as row_num from subscriptions where status = 'ACTIVE' order by row_num desc;
