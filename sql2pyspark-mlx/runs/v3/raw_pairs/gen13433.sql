SELECT s.country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.brand))) AS brand_values
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.price < 540
GROUP BY s.country
ORDER BY country ASC
LIMIT 1
