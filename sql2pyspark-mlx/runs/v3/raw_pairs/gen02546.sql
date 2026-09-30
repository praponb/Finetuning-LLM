SELECT * FROM (SELECT brand, MIN(price) AS min_price FROM products GROUP BY brand) AS x WHERE min_price >= 950 ORDER BY min_price DESC, brand DESC
