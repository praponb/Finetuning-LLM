WITH numbered AS (
    SELECT
        id,
        txn_type,
        txn_time,
        amount,
        DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY txn_time DESC) AS rn
    FROM transactions
)
SELECT *
FROM numbered
WHERE rn <= 3
ORDER BY rn DESC, amount DESC;
