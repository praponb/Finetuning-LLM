SELECT
    CASE WHEN o.discount >= 0.32 THEN 'high' WHEN o.discount >= 0.1 THEN 'mid' ELSE 'low' END AS discount_bucket,
    CASE WHEN o.shipping_fee >= 37 THEN 'high' WHEN o.shipping_fee >= 19 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket,
    COUNT(DISTINCT oi.quantity) AS unique_orders,
    COUNT(oi.unit_price) AS unit_price_count
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
GROUP BY CASE WHEN o.discount >= 0.32 THEN 'high' WHEN o.discount >= 0.1 THEN 'mid' ELSE 'low' END, CASE WHEN o.shipping_fee >= 37 THEN 'high' WHEN o.shipping_fee >= 19 THEN 'mid' ELSE 'low' END;
