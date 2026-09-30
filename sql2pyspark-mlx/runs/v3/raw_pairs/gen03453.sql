SELECT c.is_active, ROUND(AVG(a.balance), 2) AS avg_balance FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id GROUP BY c.is_active
