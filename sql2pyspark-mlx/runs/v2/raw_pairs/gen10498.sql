SELECT account_id
FROM transactions
WHERE account_id IN (4, 17, 23)
UNION
SELECT account_id
FROM transactions
WHERE txn_type IN ('CREDIT', 'DEBIT')
ORDER BY account_id
