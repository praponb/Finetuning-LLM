SELECT MIN(cost) AS min_cost, MIN(price) AS min_price, SUM(CASE WHEN brand = 'Acme' THEN price ELSE 0 END) AS acme_price FROM products ORDER BY min_price DESC;
