SELECT id, category, launch_date, price, price - LAG(price, 1) OVER (PARTITION BY category ORDER BY launch_date, id) AS price_change FROM products ORDER BY price_change, category;
