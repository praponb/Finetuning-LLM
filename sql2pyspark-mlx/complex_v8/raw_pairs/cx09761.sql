WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month
    FROM stock_moves
    WHERE qty >= 189
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.moved_at_month,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        sk.weight_kg AS sku_weight_kg
    FROM recent s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND UPPER(TRIM(w.wh_code)) LIKE 'B%'
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY MONTH(warehouse_opened_on)) AS rows_in_warehouse_opened_on_month,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_id), 0), 4) AS qty_share_of_sku_id,
        LAST_VALUE(moved_at_month) OVER (PARTITION BY sku_id, operator ORDER BY moved_at_month NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at_month,
        DENSE_RANK() OVER (ORDER BY sku_weight_kg DESC) AS sku_weight_kg_dense_rank,
        NTH_VALUE(qty, 2) OVER (PARTITION BY YEAR(moved_at_month), operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        ROUND(AVG(sku_weight_kg) OVER w_warehouse_opened_on_month, 2) AS warehouse_opened_on_month_avg_sku_weight_kg,
        LAG(warehouse_opened_on) OVER (PARTITION BY sku_id, operator ORDER BY moved_at_month NULLS LAST, id) AS prev_warehouse_opened_on,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY sku_id, operator ORDER BY moved_at_month NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        LAG(qty, 2, 0) OVER (PARTITION BY sku_id, operator ORDER BY moved_at_month NULLS LAST, id) AS prev2_qty,
        ROUND(sku_weight_kg - AVG(sku_weight_kg) OVER (PARTITION BY warehouse_id, sku_id), 2) AS sku_weight_kg_vs_warehouse_id_sku_id_avg
    FROM combined_details_warehouse
    WINDOW w_warehouse_opened_on_month AS (PARTITION BY MONTH(warehouse_opened_on))
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        qty,
        operator,
        moved_at_month,
        warehouse_wh_code,
        warehouse_opened_on,
        rows_in_warehouse_opened_on_month,
        qty_share_of_sku_id,
        sku_weight_kg_dense_rank,
        warehouse_opened_on_month_avg_sku_weight_kg,
        prev_warehouse_opened_on,
        prev2_qty,
        LAST_VALUE(moved_at_month) IGNORE NULLS OVER (PARTITION BY sku_weight_kg_dense_rank, sku_id ORDER BY sku_weight_kg_vs_warehouse_id_sku_id_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_month,
        ROUND(second_qty - AVG(second_qty) OVER w_sku_weight_kg_dense_rank, 2) AS second_qty_vs_sku_weight_kg_dense_rank_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(warehouse_opened_on) ORDER BY warehouse_opened_on), 3) AS warehouse_opened_on_cume_dist,
        MAX(qty) OVER (PARTITION BY YEAR(warehouse_opened_on) ORDER BY warehouse_opened_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_max_2_prior,
        RANK() OVER w_sku_weight_kg_dense_rank_sku_2 AS sku_weight_kg_vs_warehouse_id_sku_id_avg_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_weight_kg_dense_rank, sku_id ORDER BY sku_weight_kg_vs_warehouse_id_sku_id_avg), 3) AS sku_weight_kg_vs_warehouse_id_sku_id_avg_cume_di,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY YEAR(warehouse_opened_on) ORDER BY warehouse_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_weight_kg_dense_rank)) AS distinct_warehouse_id_in_sku_weight_kg_dense_ran
    FROM sequenced
    WINDOW w_sku_weight_kg_dense_rank AS (PARTITION BY sku_weight_kg_dense_rank), w_sku_weight_kg_dense_rank_sku_2 AS (PARTITION BY sku_weight_kg_dense_rank, sku_id ORDER BY sku_weight_kg_vs_warehouse_id_sku_id_avg, id)
)
-- Final projection
SELECT
    id,
    warehouse_wh_code,
    warehouse_opened_on,
    rows_in_warehouse_opened_on_month,
    qty_share_of_sku_id,
    sku_weight_kg_dense_rank,
    warehouse_opened_on_month_avg_sku_weight_kg,
    prev_warehouse_opened_on,
    warehouse_opened_on_cume_dist,
    qty_max_2_prior,
    sku_weight_kg_vs_warehouse_id_sku_id_avg_rank,
    sku_weight_kg_vs_warehouse_id_sku_id_avg_cume_di,
    last_known_qty,
    distinct_warehouse_id_in_sku_weight_kg_dense_ran,
    LEAST(sku_weight_kg_dense_rank, qty) AS least_sku_weight_kg_dense_rank_qty
FROM with_trends
