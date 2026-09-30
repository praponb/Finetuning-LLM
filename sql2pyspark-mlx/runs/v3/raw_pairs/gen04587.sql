SELECT ROUND(STDDEV(amount), 2) AS stddev_amount, COUNT(*) AS cnt
FROM transactions;
