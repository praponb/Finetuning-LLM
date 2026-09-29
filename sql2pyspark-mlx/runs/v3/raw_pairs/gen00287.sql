SELECT
    id,
    quantity,
    unit_price,
    product_id,
    CAST(quantity AS DOUBLE) AS quantity_str
FROM order_items
ORDER BY quantity_str DESC, id
LIMIT 20;
