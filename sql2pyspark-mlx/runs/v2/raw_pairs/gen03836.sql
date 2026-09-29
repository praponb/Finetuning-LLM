SELECT DATE_TRUNC('MONTH', a.opened_date) AS opened_month, a.account_type, MAX(t.amount) - MIN(t.amount) AS amount_range
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE a.opened_date BETWEEN '2023-07-22' AND '2024-12-01'
GROUP BY DATE_TRUNC('MONTH', a.opened_date), a.account_type
