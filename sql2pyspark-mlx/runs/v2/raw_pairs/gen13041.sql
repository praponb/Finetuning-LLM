SELECT s.city, COUNT(o.discount) AS discount_count, ROUND(STDDEV(o.amount), 2) AS stddev_amount FROM orders o LEFT JOIN stores s ON o.store_id = s.id GROUP BY s.city;
