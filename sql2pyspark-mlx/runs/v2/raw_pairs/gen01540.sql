SELECT
    id,
    order_id,
    quantity,
    product_id
FROM (
    SELECT
        id,
        order_id,
        quantity,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rn
    FROM order_items
) AS t
WHERE rn <= 3
ORDER BY quantity DESC
