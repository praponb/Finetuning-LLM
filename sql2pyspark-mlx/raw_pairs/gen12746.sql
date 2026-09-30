SELECT account_id, txn_type
FROM transactions
WHERE txn_type NOT IN ('FEE', 'REFUND')
EXCEPT
SELECT account_id, txn_type
FROM transactions
WHERE merchant IN ('Starbucks', 'Lazada', 'Amazon')
ORDER BY account_id;
