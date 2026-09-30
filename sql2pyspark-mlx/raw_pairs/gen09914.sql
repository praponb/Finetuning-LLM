SELECT id, category, price, MIN(price) OVER (PARTITION BY category) AS category_min_price FROM products;
