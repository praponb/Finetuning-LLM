WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN unit_price >= 292.3 THEN 'large' WHEN unit_price >= 238.5 THEN 'medium' ELSE 'small' END AS unit_price_tier,
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix,
        LEAST(unit_price, qty) AS least_unit_price_qty
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-02-12 11:00:00'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY move_type, warehouse_id ORDER BY least_unit_price_qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY move_type, warehouse_id ORDER BY least_unit_price_qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at
    FROM filtered_moves
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(unit_price_tier, '#', CAST(sku_id AS STRING)) AS unit_price_tier_ref,
        CONCAT_WS(': ', move_type, unit_price_tier) AS move_type_unit_price_tier_label,
        ROUND((UNIX_TIMESTAMP(second_moved_at) - UNIX_TIMESTAMP(moved_at)) / 60, 1) AS minutes_moved_at_to_second_moved_at
    FROM metrics
    WHERE second_moved_at < TIMESTAMP '2025-02-12 13:00:00'
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    sku_id,
    MIN(moved_at) AS first_moved_at,
    SUM(qty) AS total_qty,
    MAX(last_known_unit_price) AS max_last_known_unit_price
FROM tagged
GROUP BY GROUPING SETS ((move_type, sku_id), (move_type))
ORDER BY move_type NULLS LAST, sku_id NULLS LAST
