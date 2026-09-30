WITH account_stats AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Amazon' THEN amount ELSE 0 END) AS amazon_amount, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.opened_date,
    as.amazon_amount,
    as.amount_range
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
ORDER BY a.id
LIMIT 50;
