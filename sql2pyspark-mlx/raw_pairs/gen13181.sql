SELECT MAX(amount) AS max_amount, MAX(txn_time) AS latest_txn_time FROM transactions WHERE amount <= 3400 AND txn_type = 'REFUND' ORDER BY max_amount DESC;
