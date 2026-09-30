WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        unit_price - qty AS unit_price_minus_qty,
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-03-02'
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM valid_moves s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
        )
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        CONCAT(move_type_clean, '#', CAST(warehouse_id AS STRING)) AS move_type_clean_ref
    FROM filtered
),
unique_rows AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        move_type_clean_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id DESC) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
),
sku_id_stats AS (
    -- Max qty per sku_id
    SELECT sku_id, MAX(qty) AS max_qty
    FROM unique_rows
    GROUP BY sku_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_qty, ROUND(s.qty - g.max_qty, 2) AS qty_minus_max
    FROM unique_rows s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.qty = g.max_qty
),
latest_per_key AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        move_type_clean_ref,
        max_qty,
        qty_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id DESC) AS rn
        FROM outliers
    ) ranked
    WHERE rn = 1
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.move_type_clean_ref,
        s.max_qty,
        s.qty_minus_max,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.move_type
    FROM latest_per_key s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    id,
    sku_id,
    unit_price,
    qty_minus_max,
    move_id,
    move_warehouse_id,
    move_type,
    COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
FROM line_level_moves
WHERE move_type IS NOT NULL
ORDER BY sku_id DESC NULLS FIRST
