SELECT id, customer_id, opened_date, LAG(opened_date, 1) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS prev_opened_date FROM accounts
