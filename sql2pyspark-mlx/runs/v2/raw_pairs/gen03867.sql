SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE HOUR(txn_time) BETWEEN 7 AND 11) PIVOT (MAX(amount) FOR merchant IN ('Amazon', '7-Eleven', 'Grab'));
