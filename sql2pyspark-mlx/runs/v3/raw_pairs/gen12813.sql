SELECT ROUND(STDDEV(amount), 2) AS stddev_amount, MAX(discount) - MIN(discount) AS discount_range FROM orders ORDER BY stddev_amount DESC
