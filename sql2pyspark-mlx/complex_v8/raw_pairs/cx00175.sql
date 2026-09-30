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
        CASE WHEN operator IN ('op_dew', 'op_chan') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE unit_price BETWEEN 73.0 AND 317.0
      AND qty <= 349
),
warehouse_id_stats AS (
    -- Max unit_price per warehouse_id
    SELECT warehouse_id, MAX(unit_price) AS max_unit_price
    FROM base
    GROUP BY warehouse_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_unit_price,
        ROUND(s.unit_price - g.max_unit_price, 2) AS unit_price_minus_max
    FROM base s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.unit_price = g.max_unit_price
),
deduped AS (
    -- Keep the earliest row per sku_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_group,
        max_unit_price,
        unit_price_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY max_unit_price, id) AS row_num
        FROM outliers
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY unit_price_minus_max, id), 3) AS unit_price_minus_max_percent_rank,
        LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id) AS prev_moved_at,
        SIZE(COLLECT_SET(operator_group) OVER (PARTITION BY warehouse_id)) AS distinct_operator_group_in_warehouse_id,
        unit_price_minus_max - LAG(unit_price_minus_max) OVER w_warehouse_id_by_unit_price_m AS unit_price_minus_max_change,
        MAX(max_unit_price) OVER w_warehouse_id AS warehouse_id_max_max_unit_price,
        SIZE(COLLECT_SET(operator) OVER w_warehouse_id) AS distinct_operator_in_warehouse_id
    FROM deduped
    WINDOW w_warehouse_id_by_unit_price_m AS (PARTITION BY warehouse_id ORDER BY unit_price_minus_max, id), w_warehouse_id AS (PARTITION BY warehouse_id)
)
-- Final projection
SELECT DISTINCT operator_group
FROM with_trends
ORDER BY operator_group NULLS LAST
