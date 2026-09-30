WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS pos
    FROM transactions
    WHERE txn_type IN ('REFUND', 'CREDIT')
)
SELECT *
FROM Ranked
WHERE pos <= 2
ORDER BY txn_type ASC;
