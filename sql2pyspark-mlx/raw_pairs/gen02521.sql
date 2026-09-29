SELECT s.city, COUNT(p.id) AS product_count, SORT_ARRAY(COLLECT_SET(p.brand)) AS brand_list
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
WHERE p.brand IN ('Umbrella', 'Stark')
GROUP BY s.city
ORDER BY s.city
