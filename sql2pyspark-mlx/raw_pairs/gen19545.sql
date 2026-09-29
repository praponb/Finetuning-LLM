SELECT CASE WHEN p.brand IN ('Acme', 'Globex') THEN 'tier_1' ELSE 'other' END AS brand_group, ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
LEFT JOIN orders o ON o.id = oi.order_id
WHERE oi.order_id IN (7, 20, 34)
GROUP BY CASE WHEN p.brand IN ('Acme', 'Globex') THEN 'tier_1' ELSE 'other' END
ORDER BY brand_group ASC
LIMIT 5;
