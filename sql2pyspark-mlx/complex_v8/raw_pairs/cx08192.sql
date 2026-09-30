WITH recent_moves AS (
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
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE move_type NOT IN ('OUTBOUND', 'ADJUST')
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.sku_code,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM recent_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        qty,
        moved_at,
        unit_price,
        sku_labels,
        sku_category,
        RANK() OVER (ORDER BY qty NULLS LAST) AS qty_rank,
        SUM(unit_price) OVER (PARTITION BY sku_id ORDER BY qty DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_sum_6_prior,
        SUM(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        COUNT(*) OVER (PARTITION BY sku_id ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(4) OVER w_all_by_qty AS qty_quartile,
        FIRST_VALUE(qty) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, sku_category), 2) AS unit_price_vs_sku_id_sku_category_avg,
        MAX(unit_price) OVER (PARTITION BY sku_id, sku_category) AS sku_id_sku_category_max_unit_price,
        SUM(qty) OVER (PARTITION BY sku_id, sku_category) AS sku_id_sku_category_sum_qty,
        RANK() OVER (ORDER BY qty NULLS LAST, id) AS qty_rank_2,
        ROW_NUMBER() OVER (ORDER BY qty NULLS LAST, id) AS seq_no,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id_sku_category) AS distinct_warehouse_id_in_sku_id_sku_category,
        ROUND(PERCENT_RANK() OVER (ORDER BY qty NULLS LAST), 3) AS qty_percent_rank,
        ROUND(CUME_DIST() OVER w_sku_id_by_qty, 3) AS qty_cume_dist
    FROM enriched_sku
    WINDOW w_all_by_qty AS (ORDER BY qty NULLS LAST, id), w_sku_id_sku_category AS (PARTITION BY sku_id, sku_category), w_sku_id_by_qty AS (PARTITION BY sku_id ORDER BY qty DESC, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(running_unit_price) OVER (PARTITION BY distinct_warehouse_id_in_sku_id_sku_category) AS distinct_warehouse_id_in_sku_id_sku_category_max,
        SIZE(COLLECT_SET(sku_category) OVER (PARTITION BY distinct_warehouse_id_in_sku_id_sku_category)) AS distinct_sku_category_in_distinct_warehouse_id_i,
        MIN(qty_rank) OVER (PARTITION BY distinct_warehouse_id_in_sku_id_sku_category) AS distinct_warehouse_id_in_sku_id_sku_category_min,
        ROUND(CUME_DIST() OVER w_all_by_qty_rank, 3) AS qty_rank_cume_dist,
        COUNT(*) OVER (PARTITION BY distinct_warehouse_id_in_sku_id_sku_category) AS rows_in_distinct_warehouse_id_in_sku_id_sku_cate
    FROM with_trends
    WINDOW w_all_by_qty_rank AS (ORDER BY qty_rank, id), w_distinct_warehouse_id_in_sku AS (PARTITION BY distinct_warehouse_id_in_sku_id_sku_category)
)
-- Final projection
SELECT
    id,
    unit_price,
    sku_category,
    qty_rank,
    unit_price_sum_6_prior,
    running_unit_price,
    running_count,
    qty_quartile,
    unit_price_vs_sku_id_sku_category_avg,
    sku_id_sku_category_max_unit_price,
    sku_id_sku_category_sum_qty,
    qty_rank_2,
    seq_no,
    distinct_warehouse_id_in_sku_id_sku_category,
    qty_percent_rank,
    distinct_sku_category_in_distinct_warehouse_id_i,
    distinct_warehouse_id_in_sku_id_sku_category_min,
    qty_rank_cume_dist
FROM metrics
WHERE rows_in_distinct_warehouse_id_in_sku_id_sku_cate >= 7
ORDER BY sku_category NULLS LAST
