SELECT id, account_id, txn_type, WEEKOFYEAR(txn_time) AS txn_time_weekofyear FROM transactions WHERE txn_type = 'REFUND';
