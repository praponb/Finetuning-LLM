SELECT brand, ROUND(STDDEV(price), 2) AS stddev_price FROM products GROUP BY brand
