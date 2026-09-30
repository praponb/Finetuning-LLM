SELECT
    id,
    amount,
    txn_type,
    account_id
FROM transactions
WHERE amount = (SELECT MIN(amount) FROM transactions);
