SELECT
    CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS cnt,
    ROUND(SUM(quantity) * 100.0 / SUM(SUM(quantity)) OVER (), 2) AS pct_of_total
FROM order_items
WHERE order_id = 32
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
ORDER BY pct_of_total DESC
