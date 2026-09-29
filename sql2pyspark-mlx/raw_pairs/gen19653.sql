SELECT customer_id, COUNT(balance) AS balance_count FROM accounts WHERE YEAR(opened_date) = 2025 AND balance < 72000 GROUP BY customer_id ORDER BY balance_count DESC;
