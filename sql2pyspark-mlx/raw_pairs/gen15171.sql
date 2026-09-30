SELECT *
FROM (
    SELECT
        id,
        merchant,
        txn_time,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) rnk
    FROM transactions
) ranked
WHERE rnk <= 2
ORDER BY 1 DESC
