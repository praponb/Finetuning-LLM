SELECT
    id,
    product_id,
    unit_price,
    COALESCE(quantity, 0) AS quantity_filled
FROM order_items
WHERE product_id IN (8, 9) OR order_id IN (13, 14)
ORDER BY id DESC;
