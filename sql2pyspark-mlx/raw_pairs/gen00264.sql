SELECT id, opened_date, customer_id, LEAD(opened_date, 1) OVER (PARTITION BY MONTH(opened_date), customer_id ORDER BY opened_date, id) AS next_opened_date FROM accounts;
