SELECT *
FROM (
    SELECT customer_id, ROUND((100 * SUM(CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_loan, MAX(balance) - MIN(balance) AS balance_range
    FROM accounts
    GROUP BY customer_id
) AS s
WHERE pct_loan > 28
ORDER BY customer_id DESC;
