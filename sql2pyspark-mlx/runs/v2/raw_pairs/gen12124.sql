SELECT MONTH(txn_time) AS txn_month, ROUND(AVG(amount), 2) AS avg_amount FROM transactions WHERE txn_time < '2025-10-01' GROUP BY MONTH(txn_time);
