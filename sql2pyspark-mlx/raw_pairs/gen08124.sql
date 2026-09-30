SELECT account_id, merchant
FROM transactions
WHERE txn_time < '2023-09-01'
INTERSECT
SELECT account_id, merchant
FROM transactions
WHERE txn_type IN ('DEBIT', 'CREDIT', 'FEE');
