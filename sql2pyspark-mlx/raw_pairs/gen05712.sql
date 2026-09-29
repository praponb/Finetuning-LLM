SELECT brand, MIN(cost) min_cost, MIN(price) min_price FROM products WHERE price BETWEEN 520 AND 670 GROUP BY brand ORDER BY min_price DESC, min_cost DESC, brand LIMIT 20;
