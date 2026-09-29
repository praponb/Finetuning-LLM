SELECT
    id,
    product_id,
    quantity,
    unit_price,
    ROUND(unit_price, 1) AS unit_price_rounded
FROM order_items
WHERE unit_price BETWEEN 210 AND 220
