SELECT ROUND(STDDEV(discount), 2) AS stddev_discount FROM orders WHERE shipping_fee BETWEEN 26 AND 38 AND order_date >= '2025-03-01' ORDER BY stddev_discount DESC;
