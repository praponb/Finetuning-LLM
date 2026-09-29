SELECT
    id,
    product_id,
    quantity,
    COALESCE(unit_price, 0) AS unit_price_filled
FROM order_items
WHERE product_id IS NOT NULL AND product_id IS NOT NULL
