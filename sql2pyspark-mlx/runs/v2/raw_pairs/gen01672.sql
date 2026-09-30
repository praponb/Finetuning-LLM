SELECT
    order_id,
    CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    SUM(quantity) AS total_quantity,
    MAX(unit_price) AS max_unit_price
FROM order_items
GROUP BY order_id, CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
ORDER BY order_id, total_quantity;
