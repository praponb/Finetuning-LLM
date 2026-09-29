SELECT
    p.category,
    CASE WHEN p.stock_qty >= 355 THEN 'high' WHEN p.stock_qty >= 143 THEN 'mid' ELSE 'low' END stock_qty_bucket,
    MIN(oi.unit_price) min_unit_price,
    ROUND(AVG(oi.quantity), 2) avg_quantity
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY p.category, CASE WHEN p.stock_qty >= 355 THEN 'high' WHEN p.stock_qty >= 143 THEN 'mid' ELSE 'low' END
ORDER BY p.category ASC;
