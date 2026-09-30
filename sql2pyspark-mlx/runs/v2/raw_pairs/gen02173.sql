SELECT status, QUARTER(opened_date) AS opened_quarter, SUM(balance) AS total_balance
FROM accounts
WHERE status = 'CLOSED' AND customer_id IS NOT NULL
GROUP BY status, QUARTER(opened_date)
