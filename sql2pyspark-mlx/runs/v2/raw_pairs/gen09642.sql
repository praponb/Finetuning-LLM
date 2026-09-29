SELECT
    id,
    product_id,
    quantity,
    order_id,
    ROUND(unit_price, 1) AS unit_price_rounded
FROM order_items
WHERE quantity <> 3
ORDER BY quantity DESC;
