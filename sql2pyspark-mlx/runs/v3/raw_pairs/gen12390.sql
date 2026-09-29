WITH ranked_transactions AS (
    SELECT
        id,
        account_id,
        txn_time,
        txn_type,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS rn
    FROM transactions
)
SELECT *
FROM ranked_transactions
WHERE rn <= 1;
