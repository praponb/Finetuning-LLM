SELECT
    status,
    account_type,
    unique_statuss,
    avg_balance
FROM (
    SELECT
        status,
        account_type,
        COUNT(DISTINCT balance) AS unique_statuss,
        ROUND(AVG(balance), 2) AS avg_balance
    FROM accounts
    WHERE status = 'CLOSED'
    GROUP BY status, account_type
) AS x
WHERE avg_balance < 27
ORDER BY 4 DESC, status ASC, account_type ASC
LIMIT 5;
