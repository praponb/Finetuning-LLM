SELECT
    product_id,
    CASE WHEN unit_price >= 450 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    SUM(unit_price) AS total_unit_price
FROM order_items
WHERE quantity >= 4 AND quantity = 4
GROUP BY product_id, CASE WHEN unit_price >= 450 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END
HAVING ROUND(AVG(quantity), 2) > 41
ORDER BY unit_price_bucket DESC
