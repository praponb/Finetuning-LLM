SELECT *
FROM (
    SELECT
        id,
        account_id,
        txn_time,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS row_num
    FROM transactions
) AS ranked
WHERE row_num <= 2;
