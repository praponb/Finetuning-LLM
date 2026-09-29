SELECT * FROM transactions WHERE merchant = 'Shell' AND HOUR(txn_time) BETWEEN 7 AND 11 AND HOUR(txn_time) BETWEEN 10 AND 13
