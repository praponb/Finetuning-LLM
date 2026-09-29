SELECT id, merchant
FROM transactions
WHERE txn_type IN ('REFUND', 'DEBIT')
INTERSECT
SELECT id, merchant
FROM transactions
WHERE merchant != 'Amazon';
