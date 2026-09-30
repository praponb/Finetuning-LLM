SELECT
    transactions.id,
    transactions.merchant,
    transactions.txn_type,
    transactions.is_flagged
FROM transactions
WHERE transactions.account_id IN (SELECT id FROM accounts WHERE account_type <> 'CREDIT')
