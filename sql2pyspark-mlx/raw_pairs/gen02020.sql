WITH kept_keys AS (
    SELECT product_id
    FROM order_items
    INTERSECT ALL
    SELECT product_id
    FROM order_items
    WHERE quantity <> 5
),
reconciled AS (
    SELECT s.*
    FROM order_items s
    INNER JOIN kept_keys k ON s.product_id = k.product_id
)
SELECT
    id,
    quantity,
    unit_price
FROM reconciled;
