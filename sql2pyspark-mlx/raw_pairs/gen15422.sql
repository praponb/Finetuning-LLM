WITH kept_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM order_items
    EXCEPT DISTINCT
    SELECT order_id, id
    FROM order_items
    WHERE quantity <> 7
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    CORR(unit_price, quantity) AS corr_unit_price_quantity,
    CORR(unit_price, quantity) AS corr_unit_price_quantity_2,
    SUM(unit_price) AS total_unit_price
FROM reconciled
GROUP BY order_id
HAVING SUM(unit_price) >= 0
ORDER BY total_unit_price NULLS FIRST
