WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, quantity - unit_price AS line_total
    FROM order_items
    WHERE order_id = 28
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY line_total DESC NULLS FIRST, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
cleaned_order_items AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        unit_price,
        line_total,
        rn,
        line_total - quantity AS line_total_minus_quantity
    FROM latest
    WHERE unit_price IS NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, order_id
    FROM cleaned_order_items
    EXCEPT DISTINCT
    SELECT id, order_id
    FROM order_items
    WHERE order_id IN (7, 8, 19)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned_order_items s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
)
SELECT
    s.order_id,
    s.unit_price,
    s.rn,
    s.line_total_minus_quantity,
    (SELECT SUM(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS sum_payment_amount
FROM reconciled s
ORDER BY line_total_minus_quantity DESC;
