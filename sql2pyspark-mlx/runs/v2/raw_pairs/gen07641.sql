SELECT account_type, MONTH(opened_date) opened_month, MAX(balance) - MIN(balance) balance_range
FROM accounts
WHERE customer_id = 40
GROUP BY account_type, opened_month
ORDER BY opened_month
