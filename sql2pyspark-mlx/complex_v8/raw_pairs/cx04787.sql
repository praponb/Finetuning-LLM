WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day
    FROM stock_moves
    WHERE operator IN ('op_dew', 'op_eve')
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_day,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM prepared s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        SUM(unit_price) OVER w_sku_id AS sku_id_sum_unit_price,
        LAST_VALUE(warehouse_opened_on) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_warehouse_opened_on,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS unit_price_vs_sku_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(moved_at) ORDER BY unit_price DESC NULLS FIRST, id), 3) AS unit_price_percent_rank,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY TO_DATE(moved_at) ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        RANK() OVER (PARTITION BY operator, sku_id ORDER BY moved_at_day DESC, id) AS moved_at_day_rank,
        MIN(unit_price) OVER (PARTITION BY sku_id) AS sku_id_min_unit_price,
        MAX(unit_price) OVER (PARTITION BY sku_id) AS sku_id_max_unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_id), 0), 4) AS unit_price_share_of_sku_id,
        ROUND(AVG(unit_price) OVER w_sku_id, 2) AS sku_id_avg_unit_price
    FROM enriched_warehouse
    WINDOW w_sku_id AS (PARTITION BY sku_id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        moved_at_day_rank,
        COUNT(*) AS cnt,
        COUNT(DISTINCT warehouse_city) AS distinct_warehouse_citys,
        ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list,
        SUM(unit_price_share_of_sku_id) AS total_unit_price_share_of_sku_id,
        PERCENTILE_APPROX(moved_at_day_rank, 0.5) AS median_moved_at_day_rank
    FROM ranked
    GROUP BY moved_at_day_rank
    HAVING SUM(unit_price) > 180.0
)
-- Final projection
SELECT
    moved_at_day_rank,
    cnt,
    distinct_warehouse_citys,
    warehouse_city_list,
    total_unit_price_share_of_sku_id,
    ABS(distinct_warehouse_citys - 6) AS distinct_warehouse_citys_distance
FROM rollup_base
WHERE total_unit_price_share_of_sku_id > 0.9
ORDER BY distinct_warehouse_citys_distance DESC NULLS FIRST
