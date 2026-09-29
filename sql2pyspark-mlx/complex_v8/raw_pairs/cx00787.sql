WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per sku_id, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price DESC, id DESC) AS rn
        FROM prepared
    ) ranked
    WHERE rn = 1
),
latest AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id DESC) AS row_num
        FROM unique_rows
    ) ranked
    WHERE row_num = 1
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at,
        (SELECT SUM(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS total_move_unit_price,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at_2
    FROM latest s
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MAX(moved_at) AS last_moved_at,
        MAX(moved_at) AS last_moved_at_2
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows_2 AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        st.last_moved_at_2
    FROM summarized_rows s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    move_type,
    moved_at,
    operator,
    unit_price,
    first_moved_at,
    last_moved_at,
    COALESCE(total_move_unit_price, 0.0) AS total_move_unit_price_filled,
    COALESCE(total_move_unit_price, 0.0) AS total_move_unit_price_filled_2
FROM summarized_rows_2
WHERE first_moved_at >= TIMESTAMP '2025-01-17 09:00:00'
ORDER BY unit_price DESC
