WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(3) OVER w_move_type_by_moved_at AS moved_at_tercile,
        unit_price - LAG(unit_price) OVER w_move_type_by_moved_at AS unit_price_change,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        unit_price - LAG(unit_price) OVER w_all_by_days_since_moved_at AS unit_price_change_2,
        SUM(unit_price) OVER (PARTITION BY sku_id) AS sku_id_sum_unit_price,
        SUM(qty) OVER (PARTITION BY operator, warehouse_id) AS operator_warehouse_id_sum_qty,
        ROW_NUMBER() OVER (ORDER BY qty DESC, id) AS rn,
        NTH_VALUE(days_since_moved_at, 2) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_moved_at,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS unit_price_vs_sku_id_avg,
        ROUND(CUME_DIST() OVER w_operator_moved_at_to_date_by, 3) AS moved_at_cume_dist,
        MAX(qty) OVER (PARTITION BY operator, TO_DATE(moved_at) ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS qty_max_2,
        SUM(qty) OVER (ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty
    FROM cleaned
    WINDOW w_move_type_by_moved_at AS (PARTITION BY move_type ORDER BY moved_at DESC, id), w_all_by_days_since_moved_at AS (ORDER BY days_since_moved_at, id), w_operator_moved_at_to_date_by AS (PARTITION BY operator, TO_DATE(moved_at) ORDER BY moved_at DESC NULLS FIRST, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(rows_in_sku_id / NULLIF(unit_price, 0), 3) AS rows_in_sku_id_per_unit_price
    FROM sequenced
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    operator,
    move_type_slug,
    moved_at_tercile,
    unit_price_change,
    rows_in_sku_id,
    unit_price_change_2,
    sku_id_sum_unit_price,
    operator_warehouse_id_sum_qty,
    second_days_since_moved_at,
    unit_price_vs_sku_id_avg,
    moved_at_cume_dist,
    qty_max_2,
    running_qty,
    ROUND(moved_at_cume_dist / NULLIF(unit_price_vs_sku_id_avg, 0), 3) AS moved_at_cume_dist_per_unit_price_vs_sku_id_avg,
    TO_DATE(moved_at) AS moved_at_date
FROM enriched
