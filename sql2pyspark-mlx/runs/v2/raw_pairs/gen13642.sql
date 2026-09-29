SELECT DATE_FORMAT(txn_time, 'yyyy-MM') txn_month, MAX(amount) - MIN(amount) amount_range FROM transactions GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM') ORDER BY amount_range DESC;
