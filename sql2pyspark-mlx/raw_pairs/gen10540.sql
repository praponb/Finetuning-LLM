SELECT amount, txn_time, DAY(txn_time) AS txn_time_day FROM transactions WHERE is_flagged = TRUE AND account_id IN (19, 36) AND merchant = 'Lazada' ORDER BY txn_time_day;
