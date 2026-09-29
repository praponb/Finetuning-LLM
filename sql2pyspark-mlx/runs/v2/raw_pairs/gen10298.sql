SELECT id, account_id, txn_time
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) rnk
    FROM transactions
) t
WHERE rnk <= 2
ORDER BY account_id DESC, txn_time DESC
