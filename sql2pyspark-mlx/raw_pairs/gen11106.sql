SELECT id, order_id, quantity
FROM (
    SELECT
        id,
        order_id,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rnk
    FROM order_items
    WHERE quantity IS NOT NULL
) AS r
WHERE rnk <= 2
