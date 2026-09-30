WITH account_stats AS (
    SELECT account_id, MIN(amount) AS min_amount
    FROM transactions
    WHERE account_id = 33
    GROUP BY account_id
)
SELECT a.id, a.opened_date, as.min_amount
FROM accounts a
JOIN account_stats as ON a.id = as.account_id
WHERE as.min_amount > 1300
ORDER BY opened_date, min_amount DESC
