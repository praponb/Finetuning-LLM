SELECT
    status,
    customer_id,
    COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END) AS credit_count,
    COUNT(balance) AS balance_count
FROM accounts
WHERE MONTH(opened_date) IN (1, 8, 9)
GROUP BY status, customer_id
