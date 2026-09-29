SELECT status, stddev_discount FROM (SELECT status, ROUND(STDDEV(discount), 2) stddev_discount FROM orders GROUP BY status) x WHERE stddev_discount > 79 ORDER BY status;
