SELECT customer_id, MAX(balance) - MIN(balance) AS balance_range FROM accounts WHERE status = 'OPEN' GROUP BY customer_id;
