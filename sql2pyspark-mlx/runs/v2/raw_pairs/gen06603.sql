SELECT merchant, MAX(amount) AS max_amount, MIN(txn_time) AS min_txn_time FROM transactions WHERE merchant IN ('Grab', 'Lazada', 'Amazon') GROUP BY merchant HAVING max_amount > 1300;
