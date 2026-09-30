SELECT account_id, merchant
FROM transactions
WHERE txn_type NOT IN ('REFUND', 'DEBIT')
UNION ALL
SELECT account_id, merchant
FROM transactions
WHERE txn_type <> 'FEE'
ORDER BY merchant
