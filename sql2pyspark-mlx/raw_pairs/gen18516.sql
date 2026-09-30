SELECT
    customer_id,
    status,
    COUNT(balance) balance_count,
    MAX(balance) - MIN(balance) balance_range
FROM accounts
GROUP BY customer_id, status;
