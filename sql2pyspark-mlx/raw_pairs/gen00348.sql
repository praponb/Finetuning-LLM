SELECT SUM(amount) AS total_amount, COUNT(DISTINCT amount) AS unique_merchants, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
ORDER BY stddev_amount DESC, total_amount ASC;
