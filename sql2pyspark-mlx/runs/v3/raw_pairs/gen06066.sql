SELECT
    id,
    order_id,
    unit_price,
    CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 2 THEN 'Medium' ELSE 'Low' END AS quantity_tier
FROM order_items
WHERE product_id IN (3, 9)
