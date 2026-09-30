SELECT txn_type, txn_month, amount_count FROM (SELECT txn_type, MONTH(txn_time) txn_month, COUNT(amount) amount_count FROM transactions GROUP BY txn_type, MONTH(txn_time)) agg WHERE amount_count > 9
