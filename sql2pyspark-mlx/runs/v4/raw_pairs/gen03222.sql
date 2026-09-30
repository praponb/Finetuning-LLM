SELECT txn_type FROM transactions WHERE amount >= 3400 EXCEPT SELECT txn_type FROM transactions WHERE merchant IN ('Lazada', 'Starbucks', 'Shell') ORDER BY txn_type;
