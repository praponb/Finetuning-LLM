SELECT MONTH(txn_time) AS txn_month, MAX(amount) AS max_amount FROM transactions GROUP BY MONTH(txn_time);
