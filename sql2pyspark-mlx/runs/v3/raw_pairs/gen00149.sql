SELECT
    id,
    account_id,
    amount,
    txn_type
FROM (
    SELECT
        id,
        account_id,
        amount,
        txn_type,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS pos
    FROM transactions
) AS t
WHERE pos <= 3
ORDER BY account_id DESC;
