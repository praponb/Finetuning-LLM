WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        CASE WHEN move_type IN ('ADJUST', 'TRANSFER') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE (TO_DATE(moved_at) < DATE '2025-03-08' OR move_type IS NOT NULL)
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count_2
    FROM valid_moves s
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        MONTH(moved_at) AS moved_at_month,
        LOWER(REGEXP_REPLACE(TRIM(operator_ref), ' ', '_')) AS operator_ref_slug
    FROM profiled
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
      AND s.move_type IN ('OUTBOUND', 'TRANSFER', 'INBOUND')
)
SELECT
    id,
    move_type,
    qty,
    moved_at,
    operator_ref,
    unit_price_bucket,
    move_count,
    move_count_2,
    moved_at_month,
    operator_ref_slug,
    moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m,
    CONCAT_WS(': ', operator_ref, operator_ref_slug) AS operator_ref_operator_ref_slug_label
FROM qualified
ORDER BY qty, move_count_2 DESC
