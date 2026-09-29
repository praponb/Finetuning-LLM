SELECT ROUND(STDDEV(discount), 2) AS stddev_discount FROM orders WHERE channel IN ('web', 'store') ORDER BY stddev_discount;
