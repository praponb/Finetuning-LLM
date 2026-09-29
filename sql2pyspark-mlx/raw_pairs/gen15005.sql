SELECT
    customer_id,
    SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) AS frozen_balance,
    MIN(balance) AS min_balance,
    COUNT(*) AS cnt
FROM accounts
GROUP BY customer_id
HAVING SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) >= 69
