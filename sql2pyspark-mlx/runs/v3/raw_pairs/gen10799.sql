SELECT QUARTER(txn_time) AS txn_quarter, merchant, txn_type, MAX(amount) - MIN(amount) AS amount_range FROM transactions GROUP BY QUARTER(txn_time), merchant, txn_type;
