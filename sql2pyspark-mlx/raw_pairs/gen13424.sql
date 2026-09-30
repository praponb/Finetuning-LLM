SELECT s.city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.brand))) AS brand_values, MAX(p.price) AS max_price FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id GROUP BY s.city
