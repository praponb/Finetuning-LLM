SELECT account_id FROM transactions WHERE TO_DATE(txn_time) < '2025-03-01' UNION SELECT account_id FROM transactions WHERE merchant IN ('Starbucks', 'Grab');
