SELECT
    id,
    product_id,
    quantity,
    unit_price
FROM (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS rn
    FROM order_items
) AS t
WHERE rn = 1
ORDER BY unit_price
