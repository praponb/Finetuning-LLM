SELECT * FROM (SELECT YEAR(paid_at) AS paid_year, ROUND(STDDEV(amount), 2) AS stddev_amount FROM payments GROUP BY YEAR(paid_at)) AS t WHERE stddev_amount >= 37;
