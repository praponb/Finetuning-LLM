WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
deduped AS (
    -- Keep the latest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY moved_at DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM deduped s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(move_moved_at, 'yyyy-MM') AS move_moved_at_ym,
        warehouse_id,
        operator,
        ROUND(SUM(move_unit_price) / NULLIF(SUM(move_qty), 0), 3) AS move_unit_price_to_move_qty_ratio,
        PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids
    FROM line_level
    GROUP BY DATE_FORMAT(move_moved_at, 'yyyy-MM'), warehouse_id, operator
    HAVING COUNT(*) >= 2
)
SELECT
    move_moved_at_ym,
    warehouse_id,
    operator,
    median_unit_price,
    n_rows,
    distinct_ids
FROM summary
ORDER BY median_unit_price
