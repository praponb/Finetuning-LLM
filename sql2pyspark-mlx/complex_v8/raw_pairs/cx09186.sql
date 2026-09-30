WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE qty >= 154
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM scoped s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(warehouse_capacity) OVER (PARTITION BY operator, sku_id ORDER BY warehouse_opened_on DESC, id) AS next_warehouse_capacity,
        SUM(warehouse_capacity) OVER (PARTITION BY YEAR(warehouse_opened_on)) AS warehouse_opened_on_year_sum_warehouse_capacity,
        MAX(unit_price) OVER (PARTITION BY YEAR(warehouse_opened_on)) AS warehouse_opened_on_year_max_unit_price,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator, sku_id ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior,
        NTH_VALUE(moved_at, 2) OVER (ORDER BY unit_price ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(moved_at) ORDER BY warehouse_capacity), 3) AS warehouse_capacity_cume_dist,
        FIRST_VALUE(warehouse_opened_on) IGNORE NULLS OVER (PARTITION BY MONTH(moved_at) ORDER BY warehouse_capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_warehouse_opened_on,
        FIRST_VALUE(moved_at) OVER (PARTITION BY MONTH(moved_at) ORDER BY warehouse_capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER w_all_by_unit_price) AS days_since_prev_warehouse_opened_on,
        ROUND(CUME_DIST() OVER w_operator_sku_id_by_warehouse, 3) AS warehouse_opened_on_cume_dist,
        COUNT(*) OVER w_warehouse_opened_on_year AS rows_in_warehouse_opened_on_year,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY unit_price ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY warehouse_opened_on DESC, id) AS warehouse_opened_on_seq
    FROM joined
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price ASC NULLS LAST, id), w_operator_sku_id_by_warehouse AS (PARTITION BY operator, sku_id ORDER BY warehouse_opened_on DESC, id), w_warehouse_opened_on_year AS (PARTITION BY YEAR(warehouse_opened_on))
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(warehouse_capacity) OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_warehouse_opened_on), sku_id) AS first_known_warehouse_opened_on_month_sku_id_min,
        ROUND(AVG(unit_price_avg_3_prior) OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_warehouse_opened_on), sku_id), 2) AS first_known_warehouse_opened_on_month_sku_id_avg,
        LEAD(next_warehouse_capacity, 2) OVER (PARTITION BY sku_id, operator ORDER BY second_moved_at DESC, id) AS next2_next_warehouse_capacity
    FROM sequenced
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    unit_price,
    operator_ref,
    warehouse_opened_on,
    next_warehouse_capacity,
    warehouse_opened_on_year_sum_warehouse_capacity,
    warehouse_opened_on_year_max_unit_price,
    unit_price_avg_3_prior,
    warehouse_capacity_cume_dist,
    warehouse_opened_on_cume_dist,
    first_known_warehouse_opened_on_month_sku_id_min,
    first_known_warehouse_opened_on_month_sku_id_avg,
    next2_next_warehouse_capacity,
    ABS(warehouse_capacity - 11005) AS warehouse_capacity_distance,
    INITCAP(TRIM(move_type)) AS move_type_clean
FROM windowed
ORDER BY operator DESC NULLS FIRST
