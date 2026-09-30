WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE unit_price < 387.1
      AND sku_id <= 4
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.moved_at_hour,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg
    FROM filtered s
    JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_moved_at_hour_month, 0), 4) AS unit_price_share_of_moved_at_hour_month,
        LEAD(unit_price, 2) OVER (PARTITION BY sku_id ORDER BY sku_weight_kg DESC, id) AS next2_unit_price,
        RANK() OVER (PARTITION BY sku_category, move_type ORDER BY moved_at_hour) AS moved_at_hour_rank,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY MONTH(moved_at_hour)), 2) AS moved_at_hour_month_avg_sku_weight_kg,
        NTH_VALUE(moved_at_hour, 2) OVER (ORDER BY moved_at_hour NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_hour,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at_hour NULLS LAST, id), 3) AS moved_at_hour_percent_rank,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        SUM(qty) OVER (PARTITION BY warehouse_id ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_sum_7,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at_hour NULLS LAST, id), 3) AS moved_at_hour_cume_dist,
        NTH_VALUE(sku_weight_kg, 2) OVER (PARTITION BY sku_category, move_type ORDER BY moved_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY MONTH(moved_at_hour))) AS distinct_sku_id_in_moved_at_hour_month
    FROM joined_sku
    WINDOW w_moved_at_hour_month AS (PARTITION BY MONTH(moved_at_hour))
),
scored AS (
    -- Label and score rows
    SELECT *, YEAR(second_moved_at_hour) AS second_moved_at_hour_year
    FROM with_windows
)
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    operator,
    unit_price,
    moved_at_hour,
    sku_category,
    sku_weight_kg,
    unit_price_share_of_moved_at_hour_month,
    moved_at_hour_rank,
    moved_at_hour_month_avg_sku_weight_kg,
    second_moved_at_hour,
    moved_at_hour_percent_rank,
    rows_in_warehouse_id,
    qty_sum_7,
    moved_at_hour_cume_dist,
    distinct_sku_id_in_moved_at_hour_month,
    second_moved_at_hour_year,
    COALESCE(qty, 0) AS qty_filled
FROM scored
WHERE moved_at_hour_rank >= 4
