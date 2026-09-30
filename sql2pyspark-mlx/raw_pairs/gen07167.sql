SELECT AVG(price) AS avg_price, SUM(CASE WHEN brand = 'Stark' THEN price ELSE 0 END) AS stark_price FROM products
