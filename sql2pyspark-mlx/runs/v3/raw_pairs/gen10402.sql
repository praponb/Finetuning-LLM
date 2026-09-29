SELECT
    CASE WHEN unit_price >= 360 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    product_id,
    SUM(unit_price) AS total_unit_price,
    MAX(quantity) AS max_quantity
FROM order_items
GROUP BY unit_price_bucket, product_id
HAVING SUM(unit_price) >= 1000
