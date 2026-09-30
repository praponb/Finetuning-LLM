WITH remaining_keys AS (
    SELECT product_id, order_id
    FROM order_items
    EXCEPT DISTINCT
    SELECT product_id, order_id
    FROM order_items
    WHERE order_id IN (16, 28)
),
reconciled AS (
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
)
SELECT
    order_id,
    quantity,
    unit_price
FROM reconciled
ORDER BY order_id DESC NULLS LAST;
