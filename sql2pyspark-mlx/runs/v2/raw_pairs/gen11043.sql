SELECT txn_type
FROM transactions
WHERE merchant NOT IN ('Shell', 'Starbucks')
INTERSECT
SELECT txn_type
FROM transactions
WHERE account_id IN (9, 10, 39, 40);
