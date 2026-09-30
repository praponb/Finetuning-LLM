SELECT
    id,
    quantity,
    product_id,
    ROUND(unit_price * 0.85, 2) AS unit_price_scaled
FROM order_items
WHERE order_id IN (5, 9, 11, 35);
