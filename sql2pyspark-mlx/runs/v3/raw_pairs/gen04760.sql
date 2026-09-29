SELECT
    CASE WHEN p.cost >= 680 THEN 'high' WHEN p.cost >= 130 THEN 'mid' ELSE 'low' END cost_bucket,
    p.category,
    SUM(i.quantity) total_quantity,
    COUNT(*) cnt
FROM inventory i
JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN p.cost >= 680 THEN 'high' WHEN p.cost >= 130 THEN 'mid' ELSE 'low' END, p.category;
