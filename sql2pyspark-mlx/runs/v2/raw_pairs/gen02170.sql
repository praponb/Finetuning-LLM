WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE merchant = 'Grab'
),
summary AS (
    SELECT account_id, AVG(amount) AS avg_amount, COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END) AS refund_count
    FROM recent_transactions
    GROUP BY account_id
)
SELECT account_id, avg_amount, refund_count
FROM summary;
