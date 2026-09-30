SELECT CASE WHEN s.city IN ('Chiang Mai', 'Singapore') THEN 'core' ELSE 'other' END AS city_group, AVG(oi.quantity) AS avg_quantity, MIN(oi.unit_price) AS min_unit_price
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.city IN ('Chiang Mai', 'Singapore') THEN 'core' ELSE 'other' END
ORDER BY min_unit_price;
