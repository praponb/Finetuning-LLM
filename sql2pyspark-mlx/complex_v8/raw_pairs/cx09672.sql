WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        LOWER(TRIM(move_type)) AS move_type_clean,
        ABS(qty - 230) AS qty_distance
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-02-06 08:00:00'
      AND operator <> 'op_dew'
),
rollup_base AS (
    -- Summarise per group
    SELECT
        move_type,
        CASE WHEN unit_price >= 289.7 THEN 'A' WHEN unit_price >= 215.4 THEN 'B' ELSE 'C' END AS unit_price_tier,
        sku_id,
        COUNT(*) AS row_count,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        COUNT(*) FILTER (WHERE move_type = 'INBOUND') AS n_move_type_inbound,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        COUNT(*) FILTER (WHERE move_type = 'OUTBOUND') AS n_move_type_outbound,
        PERCENTILE_APPROX(operator_len, 0.5) AS median_operator_len
    FROM valid_moves
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_move_types, move_type, median_operator_len
    FROM rollup_base
) src
PIVOT (
    MIN(median_operator_len)
    FOR move_type IN ('OUTBOUND', 'INBOUND')
)
