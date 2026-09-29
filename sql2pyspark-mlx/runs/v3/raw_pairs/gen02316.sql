SELECT brand, SUM(CASE WHEN brand = 'Acme' THEN price ELSE 0 END) AS acme_price FROM products GROUP BY brand ORDER BY brand DESC
