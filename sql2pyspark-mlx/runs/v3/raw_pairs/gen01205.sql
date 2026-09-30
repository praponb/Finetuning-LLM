SELECT a.account_type, ROUND(AVG(t.amount), 2) AS avg_amount
FROM transactions t
FULL OUTER JOIN accounts a ON t.account_id = a.id
GROUP BY a.account_type
