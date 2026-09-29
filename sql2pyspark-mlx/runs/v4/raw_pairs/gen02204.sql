SELECT * FROM (SELECT MONTH(txn_time) AS txn_month, merchant, AVG(amount) AS avg_amount FROM transactions GROUP BY MONTH(txn_time), merchant) AS x WHERE avg_amount < 2400 ORDER BY txn_month DESC
