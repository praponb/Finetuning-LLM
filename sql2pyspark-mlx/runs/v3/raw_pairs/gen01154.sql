SELECT a.account_type, MONTH(a.opened_date) AS opened_month, MIN(t.amount) AS min_amount
FROM transactions t
JOIN accounts a ON a.id = t.account_id
GROUP BY a.account_type, MONTH(a.opened_date)
ORDER BY account_type
