SELECT id, customer_id, ROUND(balance * 1.15, 2) AS adjusted_balance FROM accounts ORDER BY customer_id DESC;
