SELECT CASE WHEN p.category IN ('Sports', 'Home') THEN 'tier_1' ELSE 'other' END AS category_group, SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN p.category IN ('Sports', 'Home') THEN 'tier_1' ELSE 'other' END
ORDER BY category_group DESC, revenue DESC;
