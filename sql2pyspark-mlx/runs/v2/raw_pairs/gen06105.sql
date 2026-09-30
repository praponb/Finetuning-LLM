select customer_id, opened_date from accounts where customer_id = 4 union all select customer_id, opened_date from accounts where month(opened_date) in (6, 8, 9);
