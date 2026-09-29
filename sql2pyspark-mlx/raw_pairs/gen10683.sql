WITH remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM order_items
    EXCEPT
    SELECT product_id, id
    FROM order_items
    WHERE order_id = 6
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    (SELECT SUM(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS sum_order_item_quantity
FROM verified s
ORDER BY quantity DESC NULLS FIRST;
