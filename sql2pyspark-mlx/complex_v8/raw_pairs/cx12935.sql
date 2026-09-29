WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price
    FROM stock_moves
    WHERE move_type IN ('TRANSFER', 'INBOUND', 'ADJUST')
      AND qty < 415
),
grouped AS (
    -- Summarise per group
    SELECT
        warehouse_id,
        ROUND(100 * COUNT(CASE WHEN move_type NOT IN ('ADJUST', 'INBOUND') THEN 1 END) / COUNT(*), 1) AS pct_move_type_not_in_adjust_inbo,
        COUNT(*) FILTER (WHERE move_type IN ('ADJUST', 'TRANSFER')) AS n_move_type_in_adjust_transfer,
        COUNT(*) FILTER (WHERE move_type IN ('OUTBOUND', 'ADJUST', 'INBOUND')) AS n_move_type_in_outbound_adjust
    FROM recent_moves
    GROUP BY warehouse_id
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.warehouse_id,
        s.n_move_type_in_adjust_transfer,
        st.id AS move_id,
        st.move_type,
        st.qty AS move_qty
    FROM grouped s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM line_level_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.moved_at >= TIMESTAMP '2025-01-13 10:00:00'
        )
      AND s.move_type IS NOT NULL
)
SELECT
    warehouse_id,
    n_move_type_in_adjust_transfer,
    move_type,
    move_qty,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
    SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
FROM without_matches
ORDER BY warehouse_id DESC
