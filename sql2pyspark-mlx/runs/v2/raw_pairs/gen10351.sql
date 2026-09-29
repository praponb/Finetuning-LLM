WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE account_id IN (5, 8, 29) AND account_id IN (15, 16)
),
summary AS (
    SELECT merchant, COUNT(*) AS transaction_count
    FROM recent_transactions
    GROUP BY merchant
)
SELECT *
FROM summary;
