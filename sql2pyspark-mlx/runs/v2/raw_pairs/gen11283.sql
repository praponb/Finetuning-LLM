WITH ranked AS (
    SELECT
        id,
        txn_type,
        txn_time,
        is_flagged,
        DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY txn_time DESC) rn
    FROM transactions
)
SELECT *
FROM ranked
WHERE rn <= 1;
