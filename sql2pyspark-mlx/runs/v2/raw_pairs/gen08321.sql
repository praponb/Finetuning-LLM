SELECT YEAR(txn_time) AS txn_year, merchant, ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
GROUP BY 1, 2
HAVING avg_amount >= 71
ORDER BY 2 DESC, txn_year
LIMIT 3;
