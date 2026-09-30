WITH ranked_order_items AS (
    SELECT
        id,
        order_id,
        unit_price,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rnk
    FROM order_items
    WHERE quantity <= 4
)
SELECT
    id,
    order_id,
    unit_price,
    product_id
FROM ranked_order_items
WHERE rnk = 1;
