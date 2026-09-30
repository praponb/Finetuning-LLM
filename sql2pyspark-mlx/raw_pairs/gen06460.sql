SELECT account_type, total_balance, total_count
FROM (
    SELECT account_type, SUM(balance) AS total_balance, COUNT(*) AS total_count
    FROM accounts
    GROUP BY account_type
) AS x
WHERE total_count >= 4
ORDER BY 2 DESC
