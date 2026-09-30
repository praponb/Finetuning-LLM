WITH valid_moves AS (
    -- Filter stock_moves
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
    WHERE operator <> 'op_chan'
),
enriched_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        w.city AS warehouse_city
    FROM valid_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        qty,
        moved_at,
        warehouse_city,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_moved_at_month, 0), 4) AS unit_price_share_of_moved_at_month,
        ROW_NUMBER() OVER (ORDER BY moved_at DESC, id) AS rn,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY MONTH(moved_at)), 2) AS unit_price_vs_moved_at_month_avg,
        LEAD(unit_price, 1, 0) OVER (ORDER BY moved_at DESC, id) AS next_unit_price,
        NTILE(5) OVER (PARTITION BY operator, warehouse_city ORDER BY qty DESC, id) AS qty_quintile,
        NTILE(10) OVER (PARTITION BY warehouse_id ORDER BY qty, id) AS qty_decile,
        LEAD(unit_price, 2) OVER w_all_by_moved_at AS next2_unit_price,
        NTILE(10) OVER (ORDER BY moved_at DESC, id) AS moved_at_decile,
        ROUND(qty - AVG(qty) OVER (PARTITION BY MONTH(moved_at)), 2) AS qty_vs_moved_at_month_avg,
        ROUND(AVG(qty) OVER (PARTITION BY MONTH(moved_at)), 2) AS moved_at_month_avg_qty,
        ROUND(AVG(qty) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_2_prior,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY MONTH(moved_at)), 0), 4) AS qty_share_of_moved_at_month,
        MAX(qty) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_max_7
    FROM enriched_warehouse
    WINDOW w_moved_at_month AS (PARTITION BY MONTH(moved_at)), w_all_by_moved_at AS (ORDER BY moved_at DESC, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE rn <= 5
      AND qty_decile < 8
)
-- Final projection
SELECT
    id,
    warehouse_id,
    moved_at,
    warehouse_city,
    unit_price_share_of_moved_at_month,
    rn,
    unit_price_vs_moved_at_month_avg,
    next_unit_price,
    qty_decile,
    moved_at_decile,
    qty_vs_moved_at_month_avg,
    moved_at_month_avg_qty,
    qty_avg_2_prior,
    qty_max_7,
    LOWER(REGEXP_REPLACE(TRIM(warehouse_city), ' ', '_')) AS warehouse_city_slug
FROM first_rows
WHERE next_unit_price <= 363.6
ORDER BY unit_price_share_of_moved_at_month DESC, unit_price_vs_moved_at_month_avg
