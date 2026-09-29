SELECT account_id FROM transactions WHERE txn_time < '2023-04-17' UNION ALL SELECT account_id FROM transactions WHERE txn_type = 'FEE';
