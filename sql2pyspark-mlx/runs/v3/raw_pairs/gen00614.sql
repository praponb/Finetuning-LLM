select id, customer_id, browser from sessions where (customer_id is not null and customer_id in (12, 22, 26)) or browser is not null order by id desc limit 5;
