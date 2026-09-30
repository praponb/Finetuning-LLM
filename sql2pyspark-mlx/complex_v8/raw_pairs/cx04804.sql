WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active
    FROM base_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND NOT ARRAY_CONTAINS(sk.labels, 'fragile')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(sku_weight_kg) OVER (ORDER BY sku_weight_kg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sku_weight_kg,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price), 3) AS unit_price_percent_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id ORDER BY sku_weight_kg ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior,
        FIRST_VALUE(unit_price) OVER (ORDER BY sku_weight_kg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_move_type_by_unit_price)) / 60, 1) AS minutes_since_prev_moved_at,
        ROUND(unit_price - AVG(unit_price) OVER w_sku_id_warehouse_id, 2) AS unit_price_vs_sku_id_warehouse_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_is_active ORDER BY qty DESC, id), 3) AS qty_percent_rank,
        ROUND(AVG(sku_weight_kg) OVER w_sku_id, 2) AS sku_id_avg_sku_weight_kg,
        COUNT(*) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LEAD(moved_at) OVER (ORDER BY sku_weight_kg DESC NULLS FIRST, id) AS next_moved_at,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        ROUND(CUME_DIST() OVER (ORDER BY sku_weight_kg DESC NULLS FIRST, id), 3) AS sku_weight_kg_cume_dist,
        COUNT(*) OVER (PARTITION BY sku_id, warehouse_id) AS rows_in_sku_id_warehouse_id
    FROM enriched
    WINDOW w_move_type_by_unit_price AS (PARTITION BY move_type ORDER BY unit_price ASC, id), w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id), w_sku_id AS (PARTITION BY sku_id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        YEAR(next_moved_at) AS next_moved_at_year,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        SUM(sku_weight_kg_cume_dist) AS total_sku_weight_kg_cume_dist,
        ROUND(STDDEV(qty_percent_rank), 2) AS stddev_qty_percent_rank,
        COUNT(DISTINCT id) AS distinct_ids
    FROM ranked
    GROUP BY YEAR(next_moved_at), DATE_TRUNC('MONTH', moved_at)
)
-- Final projection
SELECT next_moved_at_year, moved_at_month, total_sku_weight_kg_cume_dist
FROM rollup_base
