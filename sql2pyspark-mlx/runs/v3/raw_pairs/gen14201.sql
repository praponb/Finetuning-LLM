SELECT
    id,
    quantity,
    product_id,
    CASE WHEN quantity >= 9 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_level
FROM order_items
