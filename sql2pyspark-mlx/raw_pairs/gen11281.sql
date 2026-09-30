WITH kept_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM order_items
    EXCEPT
    SELECT product_id
    FROM order_items
    WHERE quantity != 4
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN kept_keys k ON s.product_id = k.product_id
)
SELECT
    s.id,
    s.order_id,
    s.quantity,
    s.unit_price,
    (SELECT MAX(x.amount) FROM payments x WHERE x.order_id = s.order_id) AS max_payment_amount
FROM kept_rows s
ORDER BY unit_price DESC NULLS FIRST
