WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price - qty AS unit_price_minus_qty,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day
    FROM stock_moves
    WHERE move_type <> 'OUTBOUND'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price_minus_qty,
        moved_at_day,
        SUM(qty) OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_sum_2_prior,
        NTILE(10) OVER (PARTITION BY warehouse_id, move_type ORDER BY qty, id) AS qty_decile,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id), 2) AS qty_vs_sku_id_avg,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY sku_id)) AS distinct_move_type_in_sku_id,
        LEAD(moved_at_day, 2) OVER (PARTITION BY warehouse_id, move_type ORDER BY qty, id) AS next2_moved_at_day
    FROM scoped_moves
),
enriched AS (
    -- Label and score rows
    SELECT *, ABS(unit_price_minus_qty - 650.4) AS unit_price_minus_qty_distance
    FROM with_trends
),
enriched_2 AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.labels AS sku_labels,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg
    FROM enriched s
    JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(next2_moved_at_day) - UNIX_TIMESTAMP(LAG(next2_moved_at_day) OVER (ORDER BY moved_at_day, id))) / 60, 1) AS minutes_since_prev_next2_moved_at_day,
        qty_decile - LAG(qty_decile) OVER (ORDER BY moved_at_day, id) AS qty_decile_change,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id ORDER BY qty_vs_sku_id_avg DESC), 3) AS qty_vs_sku_id_avg_cume_dist,
        SIZE(COLLECT_SET(move_type) OVER w_sku_id_warehouse_id) AS distinct_move_type_in_sku_id_warehouse_id
    FROM enriched_2
    WINDOW w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        qty_vs_sku_id_avg_cume_dist - LAG(qty_vs_sku_id_avg_cume_dist) OVER (PARTITION BY distinct_move_type_in_sku_id_warehouse_id, YEAR(moved_at_day) ORDER BY next2_moved_at_day, id) AS qty_vs_sku_id_avg_cume_dist_change,
        MAX(unit_price_minus_qty) OVER (PARTITION BY distinct_move_type_in_sku_id ORDER BY qty_vs_sku_id_avg_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_minus_qty_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_move_type_in_sku_id_warehouse_id, YEAR(moved_at_day) ORDER BY next2_moved_at_day, id), 3) AS next2_moved_at_day_percent_rank,
        SUM(qty_sum_2_prior) OVER (PARTITION BY distinct_move_type_in_sku_id ORDER BY qty_vs_sku_id_avg_cume_dist, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_sum_2_prior_sum_2_prior
    FROM metrics
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT moved_at_day, move_type, qty
    FROM sequenced
) src
PIVOT (
    MAX(qty)
    FOR move_type IN ('TRANSFER', 'INBOUND', 'OUTBOUND', 'ADJUST')
)
ORDER BY moved_at_day
