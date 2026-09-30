SELECT a.id, a.customer_id FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit BETWEEN 6400 AND 8900)
