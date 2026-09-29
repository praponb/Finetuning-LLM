SELECT
    id,
    account_id,
    txn_time,
    merchant
FROM (
    SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS row_num
    FROM transactions
) AS r
WHERE row_num = 1
