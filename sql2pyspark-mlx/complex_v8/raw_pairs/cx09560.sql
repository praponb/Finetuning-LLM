WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE operator NOT IN ('op_ann', 'op_eve')
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code,
        sk.category AS sku_category,
        sk.labels AS sku_labels
    FROM prepared_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        move_type,
        moved_at,
        warehouse_opened_on,
        warehouse_wh_code,
        sku_category,
        sku_labels,
        LAST_VALUE(moved_at) OVER (PARTITION BY sku_category ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        LAG(moved_at) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id) AS prev_moved_at,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_category ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        ROW_NUMBER() OVER (PARTITION BY sku_category, warehouse_id ORDER BY moved_at DESC, id) AS rn,
        RANK() OVER (PARTITION BY sku_category ORDER BY moved_at ASC) AS moved_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC), 3) AS moved_at_cume_dist,
        DENSE_RANK() OVER w_sku_category_warehouse_id_by_2 AS moved_at_dense_rank,
        LAG(moved_at) OVER (PARTITION BY sku_category ORDER BY moved_at ASC, id) AS prev_moved_at_2,
        FIRST_VALUE(warehouse_opened_on) IGNORE NULLS OVER (PARTITION BY sku_category, warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_warehouse_opened_on,
        DENSE_RANK() OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at, id) AS moved_at_dense_rank_2,
        ROUND(CUME_DIST() OVER w_sku_category_warehouse_id_by_2, 3) AS moved_at_cume_dist_2,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at, id)) AS days_since_prev_warehouse_opened_on,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY sku_category ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at
    FROM combined_details_warehouse
    WINDOW w_sku_category_warehouse_id_by_2 AS (PARTITION BY sku_category, warehouse_id ORDER BY moved_at DESC)
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM ranked
    WHERE moved_at_rank <= 2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY rn, moved_at_dense_rank_2 ORDER BY second_warehouse_opened_on), 3) AS second_warehouse_opened_on_percent_rank,
        LAG(prev_moved_at) OVER (PARTITION BY YEAR(last_known_moved_at) ORDER BY prev_moved_at_2 NULLS LAST, id) AS prev_prev_moved_at,
        SIZE(COLLECT_SET(sku_category) OVER w_second_moved_at_year) AS distinct_sku_category_in_second_moved_at_year,
        ROUND(rn / NULLIF(SUM(rn) OVER (PARTITION BY YEAR(second_moved_at)), 0), 4) AS rn_share_of_second_moved_at_year,
        ROW_NUMBER() OVER (PARTITION BY rn, moved_at_dense_rank_2 ORDER BY second_warehouse_opened_on, id) AS second_warehouse_opened_on_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY rn, moved_at_dense_rank_2 ORDER BY second_warehouse_opened_on), 3) AS second_warehouse_opened_on_cume_dist,
        ROUND(moved_at_cume_dist - AVG(moved_at_cume_dist) OVER (PARTITION BY YEAR(second_moved_at)), 2) AS moved_at_cume_dist_vs_second_moved_at_year_avg,
        ROUND(PERCENT_RANK() OVER w_last_known_moved_at_year_by_2, 3) AS prev_moved_at_2_percent_rank
    FROM top_per_group
    WINDOW w_second_moved_at_year AS (PARTITION BY YEAR(second_moved_at)), w_last_known_moved_at_year_by_2 AS (PARTITION BY YEAR(last_known_moved_at) ORDER BY prev_moved_at_2 NULLS LAST)
)
SELECT
    id,
    move_type,
    moved_at,
    warehouse_opened_on,
    warehouse_wh_code,
    sku_category,
    last_moved_at,
    prev_moved_at,
    second_warehouse_opened_on,
    rn,
    moved_at_cume_dist,
    moved_at_dense_rank,
    first_known_warehouse_opened_on,
    moved_at_dense_rank_2,
    moved_at_cume_dist_2,
    last_known_moved_at,
    days_since_prev_warehouse_opened_on,
    first_known_moved_at,
    second_warehouse_opened_on_percent_rank,
    prev_prev_moved_at,
    rn_share_of_second_moved_at_year,
    second_warehouse_opened_on_seq,
    second_warehouse_opened_on_cume_dist,
    prev_moved_at_2_percent_rank
FROM with_trends
WHERE prev_moved_at_2_percent_rank <= 0.7
ORDER BY moved_at_dense_rank_2 DESC
