SELECT
    customer_id,
    MIN(balance) AS min_balance,
    COUNT(*) AS num_accounts,
    ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
GROUP BY customer_id
HAVING MIN(balance) >= 68000
ORDER BY num_accounts ASC
