WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM stock_moves
    WHERE move_type = 'INBOUND'
      AND operator IN ('op_bee', 'op_chan', 'op_dew')
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_prefix,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day,
        UPPER(TRIM(operator)) AS operator_clean
    FROM filtered
),
annotated_2 AS (
    -- Add calculated columns
    SELECT
        *,
        MONTH(moved_at_day) AS moved_at_day_month,
        ROUND(unit_price * 1.1, 2) AS unit_price_adj,
        CASE WHEN unit_price >= 367.7 THEN 'HIGH' WHEN unit_price >= 136.0 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier
    FROM annotated
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        operator,
        operator_prefix,
        moved_at_day,
        operator_clean,
        unit_price_adj,
        unit_price_tier,
        DATE_FORMAT(moved_at_day, 'yyyy-MM-dd') AS moved_at_day_label,
        COALESCE(qty, 0) AS qty_filled
    FROM annotated_2
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) FILTER (WHERE move_type IN ('ADJUST', 'INBOUND')) AS move_move_type_count
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM tagged s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT
    id,
    sku_id,
    operator_prefix,
    moved_at_day,
    operator_clean,
    unit_price_tier,
    moved_at_day_label,
    qty_filled,
    SUBSTRING(TRIM(unit_price_tier), 1, 3) AS unit_price_tier_prefix,
    ROUND(unit_price_adj * 1.07, 2) AS unit_price_adj_adj
FROM with_stats
ORDER BY moved_at_day_label
