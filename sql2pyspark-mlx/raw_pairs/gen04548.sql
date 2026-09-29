WITH account_stats AS (
    SELECT account_id, AVG(amount) AS avg_amount, SUM(amount) AS total_amount
    FROM transactions
    WHERE is_flagged
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    as.avg_amount,
    as.total_amount
FROM accounts a
JOIN account_stats as ON a.id = as.account_id
WHERE as.total_amount > 6000
ORDER BY a.id DESC, as.avg_amount DESC;
