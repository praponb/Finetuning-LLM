SELECT a.status, ROUND(AVG(t.amount), 2) avg_amount FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id WHERE a.customer_id IS NULL GROUP BY a.status ORDER BY 2
