SELECT merchant, YEAR(txn_time) AS txn_year, MAX(amount) - MIN(amount) AS amount_range, SUM(amount) AS total_amount FROM transactions GROUP BY merchant, txn_year;
