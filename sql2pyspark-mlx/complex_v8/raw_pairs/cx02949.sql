WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE unit_price >= 164.1
),
sku_id_stats AS (
    -- Avg unit_price per sku_id
    SELECT sku_id, AVG(unit_price) AS avg_unit_price
    FROM scoped
    GROUP BY sku_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price,
        ROUND(s.unit_price - g.avg_unit_price, 2) AS unit_price_minus_avg
    FROM scoped s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.unit_price > g.avg_unit_price
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_minus_avg,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity
    FROM compared s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE LOWER(w.wh_code) LIKE '%nx%'
),
move_type_stats AS (
    -- Median qty per move_type
    SELECT move_type, PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM joined
    GROUP BY move_type
),
compared_2 AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_qty, ROUND(s.qty - g.median_qty, 2) AS qty_minus_median
    FROM joined s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.qty >= g.median_qty
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        unit_price_minus_avg,
        warehouse_wh_code,
        warehouse_capacity,
        qty_minus_median,
        LAST_VALUE(moved_at) OVER (PARTITION BY move_type, sku_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY move_type, YEAR(moved_at))) AS distinct_operator_in_move_type_moved_at_year
    FROM compared_2
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        moved_at,
        operator,
        unit_price,
        warehouse_wh_code,
        warehouse_capacity,
        last_moved_at,
        SIZE(COLLECT_SET(move_type) OVER w_distinct_operator_in_move_ty) AS distinct_move_type_in_distinct_operator_in_move_,
        MAX(qty_minus_median) OVER w_distinct_operator_in_move_ty AS distinct_operator_in_move_type_moved_at_year_max,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (ORDER BY last_moved_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_moved_at,
        ROUND(qty_minus_median - AVG(qty_minus_median) OVER (PARTITION BY distinct_operator_in_move_type_moved_at_year), 2) AS qty_minus_median_vs_distinct_operator_in_move_ty
    FROM windowed
    WINDOW w_distinct_operator_in_move_ty AS (PARTITION BY distinct_operator_in_move_type_moved_at_year)
)
SELECT id, metric, metric_value
FROM (
    SELECT id, distinct_move_type_in_distinct_operator_in_move_, warehouse_capacity
    FROM with_trends
) src
UNPIVOT (
    metric_value FOR metric IN (distinct_move_type_in_distinct_operator_in_move_ AS distinct_move_type_in_distinct_operator_in_move__2, warehouse_capacity AS warehouse_capacity_v)
) unp
ORDER BY id, metric
