SELECT DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, COUNT(DISTINCT amount) AS unique_merchants FROM transactions GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM');
