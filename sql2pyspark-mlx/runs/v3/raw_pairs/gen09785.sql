SELECT YEAR(txn_time) AS txn_year, MONTH(txn_time) AS txn_month, MIN(amount) AS min_amount FROM transactions GROUP BY YEAR(txn_time), MONTH(txn_time) ORDER BY min_amount DESC
