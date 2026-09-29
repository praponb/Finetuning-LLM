SELECT category, MIN(price) AS min_price FROM products WHERE category = 'Home' GROUP BY category ORDER BY min_price, category LIMIT 10
