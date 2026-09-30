SELECT MAX(opened_date) AS last_opened_date FROM accounts WHERE customer_id IN (14, 24) OR balance BETWEEN 40000 AND 67000 ORDER BY last_opened_date
