SELECT id, txn_type FROM transactions WHERE account_id IN (1, 2, 10) INTERSECT SELECT id, txn_type FROM transactions WHERE merchant IN ('7-Eleven', 'Lazada') ORDER BY txn_type;
