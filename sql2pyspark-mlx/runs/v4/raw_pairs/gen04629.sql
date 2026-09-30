SELECT YEAR(txn_time) txn_year, ROUND(STDDEV(amount), 2) stddev_amount, SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) credit_amount
FROM transactions
GROUP BY 1
HAVING stddev_amount < 44;
