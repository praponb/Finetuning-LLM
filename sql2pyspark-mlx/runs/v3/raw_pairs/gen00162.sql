SELECT
    id,
    product_id,
    unit_price,
    COALESCE(quantity, 0) AS quantity_filled
FROM order_items
WHERE (quantity >= 7 AND unit_price <= 180) OR order_id = 38
