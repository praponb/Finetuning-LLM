WITH cleaned_moves AS (
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
        CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref,
        CASE WHEN qty >= 372 THEN 'HIGH' WHEN qty >= 189 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-02-12 10:00:00'
      AND warehouse_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(unit_price) OVER w_qty_tier_warehouse_id AS qty_tier_warehouse_id_min_unit_price,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY qty_tier ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        ROUND(AVG(unit_price) OVER (PARTITION BY qty_tier ORDER BY qty, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_6_prior,
        LEAD(unit_price, 2, 0) OVER (PARTITION BY qty_tier ORDER BY qty, id) AS next2_unit_price,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY qty_tier ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY qty_tier, warehouse_id)) AS distinct_operator_in_qty_tier_warehouse_id
    FROM cleaned_moves
    WINDOW w_qty_tier_warehouse_id AS (PARTITION BY qty_tier, warehouse_id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(qty) OVER (PARTITION BY operator, sku_id) AS operator_sku_id_min_qty,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY operator, sku_id)) AS distinct_move_type_in_operator_sku_id,
        LAST_VALUE(moved_at) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        unit_price_avg_6_prior - LAG(unit_price_avg_6_prior) OVER w_all_by_distinct_operator_in AS unit_price_avg_6_prior_change
    FROM with_windows
    WINDOW w_all_by_distinct_operator_in AS (ORDER BY distinct_operator_in_qty_tier_warehouse_id, id)
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type = 'OUTBOUND'
        )
      AND s.operator_ref IS NOT NULL
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator,
    unit_price,
    operator_ref,
    qty_tier,
    operator_prefix,
    first_known_qty,
    unit_price_avg_6_prior,
    first_known_unit_price,
    distinct_operator_in_qty_tier_warehouse_id,
    operator_sku_id_min_qty,
    distinct_move_type_in_operator_sku_id,
    last_moved_at,
    unit_price_avg_6_prior_change,
    COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
FROM without_matches
WHERE first_known_unit_price < 307.8
ORDER BY qty NULLS LAST
