SELECT category, COUNT(cost) AS cost_count, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
FROM products
WHERE cost <= 460
GROUP BY category
HAVING COUNT(cost) > 4
ORDER BY category DESC;
