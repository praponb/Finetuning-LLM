WITH account_stats AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END) AS starbucks_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.opened_date,
    a.balance,
    as.starbucks_count
FROM accounts a
JOIN account_stats as ON a.id = as.account_id
ORDER BY starbucks_count, id
LIMIT 5;
