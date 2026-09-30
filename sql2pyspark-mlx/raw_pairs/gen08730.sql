SELECT
    account_type,
    status,
    customer_id,
    COUNT(DISTINCT balance) AS unique_statuss
FROM accounts
GROUP BY account_type, status, customer_id
ORDER BY account_type DESC;
