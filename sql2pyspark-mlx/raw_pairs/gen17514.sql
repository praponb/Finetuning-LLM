SELECT id, order_id, quantity
FROM (
    SELECT
        id,
        order_id,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) rnk
    FROM order_items
) r
WHERE rnk <= 3
ORDER BY order_id DESC, id
