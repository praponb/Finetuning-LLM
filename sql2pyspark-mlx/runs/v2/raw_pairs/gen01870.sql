SELECT
    customer_id,
    account_type,
    CASE WHEN balance >= 67000 THEN 'high' WHEN balance >= 27000 THEN 'mid' ELSE 'low' END balance_bucket,
    AVG(balance) avg_balance
FROM accounts
WHERE customer_id IN (2, 16, 18) AND opened_date < '2023-08-01' AND customer_id IN (7, 26)
GROUP BY customer_id, account_type, balance_bucket
ORDER BY customer_id DESC, account_type, balance_bucket
LIMIT 20
