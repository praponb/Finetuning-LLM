SELECT merchant, txn_type
FROM transactions
INTERSECT ALL
SELECT merchant, txn_type
FROM transactions
WHERE merchant IS NOT NULL;
