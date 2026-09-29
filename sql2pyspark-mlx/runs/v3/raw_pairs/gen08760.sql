SELECT
    id,
    product_id,
    order_id,
    quantity,
    CAST(quantity AS DOUBLE) AS quantity_str,
    CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 4 THEN 'Medium' ELSE 'Low' END AS quantity_tier
FROM order_items
WHERE quantity <= 5 AND quantity < 3
