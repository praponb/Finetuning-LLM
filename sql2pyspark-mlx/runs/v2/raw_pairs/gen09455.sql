WITH ranked_order_items AS (
    SELECT
        id,
        product_id,
        unit_price,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS pos
    FROM order_items
    WHERE product_id IN (16, 28)
)
SELECT
    id,
    product_id,
    unit_price,
    quantity
FROM ranked_order_items
WHERE pos <= 1
ORDER BY id ASC, quantity DESC;
