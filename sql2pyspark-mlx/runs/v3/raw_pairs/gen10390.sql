SELECT * FROM accounts WHERE (customer_id IN (5, 29, 40) AND customer_id IS NOT NULL) OR customer_id = 29 ORDER BY customer_id DESC, balance, id LIMIT 50
