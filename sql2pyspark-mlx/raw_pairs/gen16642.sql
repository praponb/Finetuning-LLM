SELECT
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END quantity_bucket,
    SUM(quantity) total_quantity,
    COUNT(*) cnt,
    SUM(SUM(quantity)) OVER (ORDER BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END) AS cumulative_quantity
FROM order_items
GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
