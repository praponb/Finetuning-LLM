WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type = 'INBOUND'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        MIN(unit_price) AS min_unit_price,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM filtered_moves
    WHERE warehouse_id IS NOT NULL
    GROUP BY 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        moved_at_month,
        min_unit_price,
        distinct_sku_ids,
        operator_list,
        ROUND(AVG(distinct_sku_ids) OVER (PARTITION BY YEAR(moved_at_month), distinct_sku_ids), 2) AS moved_at_month_year_distinct_sku_ids_avg_distinc,
        ROUND(AVG(min_unit_price) OVER (PARTITION BY YEAR(moved_at_month), distinct_sku_ids), 2) AS moved_at_month_year_distinct_sku_ids_avg_min_uni,
        LEAD(distinct_sku_ids) OVER (PARTITION BY MONTH(moved_at_month), distinct_sku_ids ORDER BY min_unit_price, moved_at_month) AS next_distinct_sku_ids
    FROM rollup_base
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(next_distinct_sku_ids / NULLIF(SUM(next_distinct_sku_ids) OVER (PARTITION BY distinct_sku_ids, next_distinct_sku_ids), 0), 4) AS next_distinct_sku_ids_share_of_distinct_sku_ids_,
        DENSE_RANK() OVER (ORDER BY moved_at_month_year_distinct_sku_ids_avg_min_uni) AS moved_at_month_year_distinct_sku_ids_avg_min_uni_2,
        MAX(distinct_sku_ids) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_month), next_distinct_sku_ids ORDER BY next_distinct_sku_ids ASC, moved_at_month ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS distinct_sku_ids_max_4
    FROM with_windows
),
top_per_group AS (
    -- First row per group
    SELECT
        moved_at_month,
        min_unit_price,
        distinct_sku_ids,
        operator_list,
        moved_at_month_year_distinct_sku_ids_avg_distinc,
        moved_at_month_year_distinct_sku_ids_avg_min_uni,
        next_distinct_sku_ids,
        next_distinct_sku_ids_share_of_distinct_sku_ids_,
        distinct_sku_ids_max_4
    FROM windowed
    WHERE moved_at_month_year_distinct_sku_ids_avg_min_uni_2 = 1
      AND moved_at_month_year_distinct_sku_ids_avg_min_uni <= 275.4
),
tagged AS (
    -- Label and score rows
    SELECT *, COALESCE(min_unit_price, 0.0) AS min_unit_price_filled
    FROM top_per_group
)
SELECT
    next_distinct_sku_ids,
    distinct_sku_ids,
    MIN(moved_at_month_year_distinct_sku_ids_avg_distinc) AS min_moved_at_month_year_distinct_sku_ids_avg_dis,
    MIN(moved_at_month) AS first_moved_at_month,
    ROUND(STDDEV(next_distinct_sku_ids_share_of_distinct_sku_ids_), 2) AS stddev_next_distinct_sku_ids_share_of_distinct_s
FROM tagged
GROUP BY GROUPING SETS ((next_distinct_sku_ids, distinct_sku_ids), (next_distinct_sku_ids), ())
