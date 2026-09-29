WITH account_stats AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Amazon' THEN amount ELSE 0 END) AS amazon_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    a.account_type,
    as.amazon_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id;
