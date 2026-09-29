SELECT id, merchant, WEEKOFYEAR(txn_time) txn_time_weekofyear, CAST(is_flagged AS INT) is_flagged_flag FROM transactions;
