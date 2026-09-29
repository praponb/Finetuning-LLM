WITH base AS (
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
        qty - unit_price AS qty_minus_unit_price,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-03-02'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id)) AS distinct_move_type_in_warehouse_id,
        MAX(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_unit_price
    FROM base
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(': ', move_type, operator_prefix) AS move_type_operator_prefix_label
    FROM ranked
),
move_type_stats AS (
    -- Avg unit_price per move_type
    SELECT move_type, AVG(unit_price) AS avg_unit_price
    FROM tagged
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price,
        ROUND(s.unit_price - g.avg_unit_price, 2) AS unit_price_minus_avg
    FROM tagged s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price > g.avg_unit_price
),
per_group AS (
    -- Summarise per group
    SELECT
        move_type,
        warehouse_id,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS row_count,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        ROUND(SUM(qty_minus_unit_price) / NULLIF(SUM(avg_unit_price), 0), 3) AS qty_minus_unit_price_to_avg_unit_price_ratio
    FROM outliers
    GROUP BY move_type, warehouse_id
    HAVING COUNT(*) >= 2
)
SELECT
    move_type,
    distinct_ids,
    row_count,
    distinct_warehouse_ids,
    qty_minus_unit_price_to_avg_unit_price_ratio,
    row_count + distinct_warehouse_ids AS row_count_plus_distinct_warehouse_ids
FROM per_group
ORDER BY distinct_warehouse_ids, row_count_plus_distinct_warehouse_ids
