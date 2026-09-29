SELECT a.customer_id, ROUND(AVG(t.amount), 2) AS avg_amount, COUNT(*) AS cnt
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE t.amount <= 2500
GROUP BY a.customer_id
ORDER BY avg_amount;
