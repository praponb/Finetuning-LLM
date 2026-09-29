WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix,
        moved_at + INTERVAL 1 DAYS AS moved_at_plus_1d
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 6 AND 12
),
with_parent_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.move_type_prefix,
        s.moved_at_plus_1d,
        w.city AS warehouse_city
    FROM scoped_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.city IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        warehouse_city,
        NTILE(5) OVER w_warehouse_city_by_moved_at_p AS moved_at_plus_1d_quintile,
        LEAD(moved_at_plus_1d) OVER w_warehouse_city_by_moved_at_p AS next_moved_at_plus_1d,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(moved_at_plus_1d), move_type ORDER BY moved_at_plus_1d DESC), 3) AS moved_at_plus_1d_percent_rank,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_city)) AS distinct_move_type_in_warehouse_city,
        COUNT(*) OVER (PARTITION BY warehouse_city) AS rows_in_warehouse_city,
        RANK() OVER (PARTITION BY operator, warehouse_city ORDER BY moved_at_plus_1d DESC) AS moved_at_plus_1d_rank,
        ROUND((UNIX_TIMESTAMP(moved_at_plus_1d) - UNIX_TIMESTAMP(LAG(moved_at_plus_1d) OVER (PARTITION BY MONTH(moved_at_plus_1d), move_type ORDER BY moved_at_plus_1d DESC, id))) / 60, 1) AS minutes_since_prev_moved_at_plus_1d,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_city), 2) AS warehouse_city_avg_qty,
        LAG(moved_at_plus_1d, 2) OVER w_moved_at_plus_1d_month_move_2 AS prev2_moved_at_plus_1d,
        SIZE(COLLECT_SET(sku_id) OVER w_warehouse_city) AS distinct_sku_id_in_warehouse_city,
        NTH_VALUE(moved_at_plus_1d, 2) OVER (PARTITION BY operator, warehouse_city ORDER BY moved_at_plus_1d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_plus_1d,
        ROUND(AVG(qty) OVER (PARTITION BY MONTH(moved_at_plus_1d), move_type ORDER BY moved_at_plus_1d DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_6_prior
    FROM with_parent_warehouse
    WINDOW w_warehouse_city_by_moved_at_p AS (PARTITION BY warehouse_city ORDER BY moved_at_plus_1d, id), w_moved_at_plus_1d_month_move_2 AS (PARTITION BY MONTH(moved_at_plus_1d), move_type ORDER BY moved_at_plus_1d DESC, id), w_warehouse_city AS (PARTITION BY warehouse_city)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        move_type,
        operator,
        warehouse_city,
        moved_at_plus_1d_quintile,
        next_moved_at_plus_1d,
        moved_at_plus_1d_percent_rank,
        distinct_move_type_in_warehouse_city,
        moved_at_plus_1d_rank,
        minutes_since_prev_moved_at_plus_1d,
        warehouse_city_avg_qty,
        prev2_moved_at_plus_1d,
        GREATEST(rows_in_warehouse_city, qty_avg_6_prior) AS greatest_rows_in_warehouse_city_qty_avg_6_prior,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM with_trends
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    warehouse_city,
    moved_at_plus_1d_quintile,
    next_moved_at_plus_1d,
    distinct_move_type_in_warehouse_city,
    moved_at_plus_1d_rank,
    warehouse_city_avg_qty,
    prev2_moved_at_plus_1d,
    greatest_rows_in_warehouse_city_qty_avg_6_prior,
    move_type_slug
FROM annotated
ORDER BY warehouse_city_avg_qty, id
LIMIT 10
