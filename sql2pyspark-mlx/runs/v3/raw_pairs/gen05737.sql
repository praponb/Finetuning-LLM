SELECT s.country, s.city, SORT_ARRAY(COLLECT_SET(p.brand)) brand_list
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.supplier_id IN (6, 26)
GROUP BY s.country, s.city;
