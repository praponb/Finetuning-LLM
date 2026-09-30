SELECT id, customer_id, opened_date, balance, LEAD(balance, 1) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS next_balance FROM accounts;
