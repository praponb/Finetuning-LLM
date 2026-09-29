SELECT
    order_id,
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    SUM(quantity * unit_price) AS revenue,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS total_count
FROM order_items
GROUP BY 1, 2
