SELECT
    id,
    amount,
    account_id,
    txn_type
FROM transactions
WHERE amount = (SELECT MIN(amount) FROM transactions);
