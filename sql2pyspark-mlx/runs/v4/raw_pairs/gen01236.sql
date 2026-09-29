SELECT supplier_id, ROUND(AVG(price), 2) AS avg_price FROM products WHERE ARRAY_CONTAINS(tags, 'eco') GROUP BY supplier_id ORDER BY avg_price DESC, supplier_id DESC LIMIT 10;
