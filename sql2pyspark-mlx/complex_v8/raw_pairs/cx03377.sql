WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE qty BETWEEN 99 AND 288
      AND move_type <> 'INBOUND'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type, move_type_or_unknown), 2) AS unit_price_vs_move_type_move_type_or_unknown_avg,
        COUNT(*) OVER (PARTITION BY sku_id, warehouse_id) AS rows_in_sku_id_warehouse_id,
        NTILE(5) OVER w_all_by_unit_price AS unit_price_quintile,
        LAG(moved_at, 2) OVER (PARTITION BY move_type_or_unknown ORDER BY moved_at DESC, id) AS prev2_moved_at,
        FIRST_VALUE(moved_at) OVER (PARTITION BY sku_id ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        ROUND(AVG(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_2,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type_or_unknown, move_type ORDER BY unit_price ASC), 3) AS unit_price_cume_dist,
        SIZE(COLLECT_SET(operator_or_unknown) OVER (PARTITION BY sku_id, warehouse_id)) AS distinct_operator_or_unknown_in_sku_id_warehouse,
        RANK() OVER (PARTITION BY sku_id ORDER BY unit_price DESC NULLS FIRST) AS unit_price_rank,
        FIRST_VALUE(moved_at) OVER (PARTITION BY move_type_or_unknown ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at_2,
        LEAD(unit_price) OVER (PARTITION BY operator_or_unknown ORDER BY unit_price DESC, id) AS next_unit_price,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, warehouse_id), 2) AS unit_price_vs_sku_id_warehouse_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY unit_price DESC), 3) AS unit_price_cume_dist_2,
        LEAD(unit_price, 2, 0) OVER (PARTITION BY move_type_or_unknown, move_type ORDER BY unit_price ASC, id) AS next2_unit_price
    FROM valid_moves
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price DESC, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE unit_price_rank = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        move_type_or_unknown,
        unit_price_vs_move_type_move_type_or_unknown_avg,
        rows_in_sku_id_warehouse_id,
        unit_price_quintile,
        unit_price_avg_2,
        unit_price_cume_dist,
        distinct_operator_or_unknown_in_sku_id_warehouse,
        first_moved_at_2,
        next_unit_price,
        next2_unit_price,
        NTH_VALUE(unit_price_quintile, 2) OVER (PARTITION BY move_type_or_unknown ORDER BY unit_price_avg_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY unit_price_cume_dist_2 NULLS LAST, id), 3) AS unit_price_cume_dist_2_percent_rank,
        LAST_VALUE(unit_price) OVER (PARTITION BY move_type_or_unknown ORDER BY unit_price_avg_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price
    FROM leaders
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    move_type_or_unknown,
    unit_price_vs_move_type_move_type_or_unknown_avg,
    rows_in_sku_id_warehouse_id,
    unit_price_quintile,
    unit_price_avg_2,
    unit_price_cume_dist,
    next_unit_price,
    next2_unit_price,
    second_unit_price_quintile,
    last_unit_price,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
FROM with_trends
ORDER BY moved_at, move_type DESC
