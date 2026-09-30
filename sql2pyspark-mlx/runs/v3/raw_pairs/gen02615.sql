SELECT MIN(txn_time) min_txn_time, MAX(amount) - MIN(amount) amount_range FROM transactions WHERE txn_type IN ('FEE', 'DEBIT') AND amount < 1200 ORDER BY min_txn_time, amount_range;
