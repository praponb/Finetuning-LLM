WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m
    FROM stock_moves
    WHERE qty < 387
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.category AS sku_category,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM recent s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        qty,
        operator,
        unit_price_adj,
        moved_at_plus_30m,
        sku_category,
        warehouse_wh_code,
        warehouse_city,
        SUM(unit_price) OVER (PARTITION BY warehouse_id, sku_category) AS warehouse_id_sku_category_sum_unit_price,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at_plus_30m NULLS LAST, id), 3) AS moved_at_plus_30m_percent_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_category ORDER BY unit_price DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_6_prior,
        ROUND(PERCENT_RANK() OVER w_sku_category_operator_by_qty, 3) AS qty_percent_rank,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY unit_price ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(qty) OVER w_warehouse_city, 2) AS warehouse_city_avg_qty,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY moved_at_plus_30m NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        LEAD(moved_at_plus_30m, 3) OVER w_warehouse_id_by_moved_at_plu AS next3_moved_at_plus_30m
    FROM with_details
    WINDOW w_sku_category_operator_by_qty AS (PARTITION BY sku_category, operator ORDER BY qty ASC), w_warehouse_city AS (PARTITION BY warehouse_city), w_warehouse_id_by_moved_at_plu AS (PARTITION BY warehouse_id ORDER BY moved_at_plus_30m NULLS LAST, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(warehouse_city_avg_qty) OVER w_moved_at_plus_30m_month, 2) AS moved_at_plus_30m_month_avg_warehouse_city_avg_q,
        MIN(unit_price_avg_6_prior) OVER (PARTITION BY YEAR(next3_moved_at_plus_30m) ORDER BY running_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_avg_6_prior_so_far,
        ROUND(first_known_unit_price / NULLIF(SUM(first_known_unit_price) OVER (PARTITION BY MONTH(moved_at_plus_30m)), 0), 4) AS first_known_unit_price_share_of_moved_at_plus_30,
        COUNT(*) OVER w_moved_at_plus_30m_month AS rows_in_moved_at_plus_30m_month,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY MONTH(moved_at_plus_30m))) AS distinct_warehouse_id_in_moved_at_plus_30m_month,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(next3_moved_at_plus_30m) ORDER BY running_count DESC, id), 3) AS running_count_cume_dist,
        MIN(running_count) OVER (ORDER BY warehouse_city_avg_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_running_count_so_far
    FROM with_trends
    WINDOW w_moved_at_plus_30m_month AS (PARTITION BY MONTH(moved_at_plus_30m))
)
SELECT
    id,
    warehouse_id,
    qty,
    operator,
    unit_price_adj,
    moved_at_plus_30m,
    sku_category,
    warehouse_wh_code,
    warehouse_city,
    warehouse_id_sku_category_sum_unit_price,
    moved_at_plus_30m_percent_rank,
    qty_percent_rank,
    running_count,
    first_known_unit_price,
    next3_moved_at_plus_30m,
    moved_at_plus_30m_month_avg_warehouse_city_avg_q,
    min_unit_price_avg_6_prior_so_far,
    first_known_unit_price_share_of_moved_at_plus_30,
    distinct_warehouse_id_in_moved_at_plus_30m_month,
    running_count_cume_dist,
    min_running_count_so_far,
    CASE WHEN unit_price_adj >= 487.9 THEN 'HIGH' WHEN unit_price_adj >= 338.1 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_adj_tier,
    DATE_FORMAT(moved_at_plus_30m, 'MMM yyyy') AS moved_at_plus_30m_label
FROM windowed
WHERE qty_percent_rank <= 0.6
ORDER BY next3_moved_at_plus_30m DESC, min_running_count_so_far
