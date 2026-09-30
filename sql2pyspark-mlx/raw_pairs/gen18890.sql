SELECT CASE WHEN s.city IN ('Berlin', 'Singapore') THEN 'core' ELSE 'other' END AS city_group, SUM(i.quantity) AS total_quantity
FROM inventory i
RIGHT OUTER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
WHERE p.product_name LIKE 'Des%'
GROUP BY CASE WHEN s.city IN ('Berlin', 'Singapore') THEN 'core' ELSE 'other' END
ORDER BY total_quantity DESC, city_group DESC
