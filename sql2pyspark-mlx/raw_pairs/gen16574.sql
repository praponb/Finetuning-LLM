SELECT
    id,
    account_id,
    txn_time,
    amount
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS rnk
    FROM transactions
) AS t
WHERE rnk = 1
ORDER BY amount DESC
