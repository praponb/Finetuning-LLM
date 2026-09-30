SELECT
    id,
    product_id,
    quantity,
    order_id
FROM (
    SELECT
        id,
        product_id,
        quantity,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) rn
    FROM order_items
) r
WHERE rn <= 2
ORDER BY product_id DESC
