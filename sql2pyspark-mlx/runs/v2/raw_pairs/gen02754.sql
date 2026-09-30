WITH account_stats AS (
    SELECT account_id, MIN(amount) AS min_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.customer_id, as.min_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id;
