SELECT
    id,
    account_id,
    amount,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS rn
FROM transactions
WHERE txn_type = 'REFUND';
