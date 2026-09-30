WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) pos
    FROM transactions
)
SELECT id, txn_type, amount
FROM numbered
WHERE pos <= 2
ORDER BY txn_type ASC, id ASC;
