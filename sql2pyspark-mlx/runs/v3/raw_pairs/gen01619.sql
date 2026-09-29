SELECT CASE WHEN p.category IN ('Home', 'Sports') THEN 'tier_1' ELSE 'other' END AS category_group, ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE LENGTH(p.product_name) < 13
GROUP BY CASE WHEN p.category IN ('Home', 'Sports') THEN 'tier_1' ELSE 'other' END
ORDER BY category_group, avg_unit_price DESC
