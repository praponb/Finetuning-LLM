SELECT supplier_id, COUNT(price) AS price_count, SUM(price) AS total_price FROM products WHERE supplier_id IN (13, 32, 37, 39) GROUP BY supplier_id ORDER BY supplier_id;
