SELECT
    t.id,
    t.txn_type,
    t.is_flagged,
    t.amount
FROM transactions t
WHERE t.txn_type IN ('CREDIT', 'FEE') AND t.account_id NOT IN (SELECT id FROM accounts WHERE balance BETWEEN 35000 AND 55000)
ORDER BY txn_type ASC;
