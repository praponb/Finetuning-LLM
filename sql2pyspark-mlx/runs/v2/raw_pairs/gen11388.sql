SELECT account_id
FROM transactions
WHERE txn_type NOT IN ('FEE', 'DEBIT')
INTERSECT
SELECT account_id
FROM transactions
WHERE amount BETWEEN 1300 AND 1600
ORDER BY account_id;
