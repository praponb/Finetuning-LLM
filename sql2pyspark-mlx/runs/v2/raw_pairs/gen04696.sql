SELECT s.city, COUNT(p.id) product_count, SORT_ARRAY(COLLECT_SET(p.brand)) brand_list
FROM suppliers s
INNER JOIN products p ON s.id = p.supplier_id
GROUP BY s.city
ORDER BY city, product_count;
