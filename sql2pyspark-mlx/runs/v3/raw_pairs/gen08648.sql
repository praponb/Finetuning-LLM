SELECT id, stock_qty, brand, category, ROUND(price, 2) AS price_rounded FROM products ORDER BY category, id LIMIT 10;
