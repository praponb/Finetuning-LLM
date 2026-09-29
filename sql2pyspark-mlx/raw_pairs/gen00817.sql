SELECT id, category, launch_date, price, LEAD(price, 2) OVER (PARTITION BY category ORDER BY launch_date, id) AS next_price FROM products ORDER BY category;
