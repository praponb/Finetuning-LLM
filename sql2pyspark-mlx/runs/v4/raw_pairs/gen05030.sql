SELECT s.id, COUNT(p.id) AS product_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.brand))) AS brand_values
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
GROUP BY s.id
ORDER BY product_count ASC;
