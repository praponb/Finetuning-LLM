SELECT s.country, ROUND((100 * SUM(CASE WHEN p.brand = 'Globex' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_globex, SORT_ARRAY(COLLECT_SET(p.brand)) AS brand_list
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
