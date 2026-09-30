SELECT merchant, ROUND(STDDEV(amount), 2) AS stddev_amount, AVG(amount) AS avg_amount
FROM transactions
GROUP BY merchant
ORDER BY stddev_amount DESC;
