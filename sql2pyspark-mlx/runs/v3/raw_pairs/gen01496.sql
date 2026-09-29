SELECT a.customer_id, MAX(t.amount) - MIN(t.amount) AS amount_range, AVG(t.amount) AS avg_amount
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
GROUP BY a.customer_id;
