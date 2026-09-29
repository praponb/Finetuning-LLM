WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
stats AS (
    -- Summarise per group
    SELECT
        sku_id,
        operator,
        move_type_len,
        ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
        PERCENTILE_APPROX(qty_per_unit_price, 0.5) AS median_qty_per_unit_price
    FROM scoped_moves
    GROUP BY sku_id, operator, move_type_len
    HAVING SUM(qty_per_unit_price) > 3.0
),
qualified AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM stats s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.unit_cost < 251.2
              AND sk.category NOT IN ('Electronics', 'Food')
        )
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.sku_id,
        s.operator,
        s.move_type_len,
        s.stddev_unit_price,
        s.median_qty_per_unit_price,
        st.id AS move_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at,
        st.move_type,
        st.operator AS move_operator
    FROM qualified s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    sku_id,
    operator,
    move_type_len,
    stddev_unit_price,
    median_qty_per_unit_price,
    move_id,
    move_qty,
    move_moved_at,
    move_type,
    FLOOR(move_type_len / 2) * 2 AS move_type_len_bucket,
    FLOOR(move_type_len / 2) * 2 AS move_type_len_bucket_2
FROM exploded_detail
WHERE move_id >= 56
