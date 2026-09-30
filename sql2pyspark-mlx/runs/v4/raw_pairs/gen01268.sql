WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        quantity * unit_price AS line_total,
        quantity + unit_price AS line_total_2
    FROM order_items
    WHERE unit_price > 260
),
enriched AS (
    -- Attach orders attributes
    SELECT a.*, d.discount
    FROM base a
    INNER JOIN orders d
        ON a.order_id = d.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC NULLS FIRST) AS dedup_rank
        FROM enriched
    ) t
    WHERE dedup_rank = 1
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.line_total_2,
    (SELECT MAX(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS max_inventory_quantity
FROM ranked s;
