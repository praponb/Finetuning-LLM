SELECT id, account_id, txn_time
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS pos
    FROM transactions
) AS ranked
WHERE pos <= 2
