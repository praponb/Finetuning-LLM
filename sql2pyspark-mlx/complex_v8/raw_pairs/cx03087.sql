WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id >= 5
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM recent_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(warehouse_capacity) OVER (PARTITION BY warehouse_city, YEAR(moved_at) ORDER BY warehouse_capacity, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS warehouse_capacity_max_1_prior,
        MAX(unit_price) OVER (PARTITION BY warehouse_city, YEAR(moved_at) ORDER BY warehouse_capacity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_max_3_prior,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER w_warehouse_city_moved_at_year_3) AS days_since_prev_warehouse_opened_on,
        RANK() OVER w_warehouse_opened_on_month_by AS unit_price_rank,
        MIN(warehouse_capacity) OVER w_warehouse_city AS warehouse_city_min_warehouse_capacity,
        LEAD(warehouse_opened_on, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY moved_at DESC, id) AS next2_warehouse_opened_on,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY warehouse_city, YEAR(moved_at) ORDER BY warehouse_capacity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS warehouse_capacity_avg_3_prior,
        ROUND(warehouse_capacity / NULLIF(SUM(warehouse_capacity) OVER (PARTITION BY warehouse_city, warehouse_id), 0), 4) AS warehouse_capacity_share_of_warehouse_city_wareh,
        LAST_VALUE(warehouse_opened_on) IGNORE NULLS OVER (ORDER BY warehouse_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_warehouse_opened_on,
        RANK() OVER (PARTITION BY MONTH(warehouse_opened_on) ORDER BY unit_price NULLS LAST, id) AS unit_price_rank_2,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY moved_at DESC, id) AS moved_at_quartile,
        SUM(warehouse_capacity) OVER (PARTITION BY warehouse_city, warehouse_id) AS warehouse_city_warehouse_id_sum_warehouse_capaci,
        MIN(warehouse_capacity) OVER (PARTITION BY warehouse_city, warehouse_id) AS warehouse_city_warehouse_id_min_warehouse_capaci,
        LEAD(unit_price) OVER w_all_by_warehouse_opened_on AS next_unit_price,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY moved_at DESC, id))) / 60, 1) AS minutes_since_prev_moved_at
    FROM enriched
    WINDOW w_warehouse_city_moved_at_year_3 AS (PARTITION BY warehouse_city, YEAR(moved_at) ORDER BY warehouse_capacity, id), w_warehouse_opened_on_month_by AS (PARTITION BY MONTH(warehouse_opened_on) ORDER BY unit_price NULLS LAST), w_warehouse_city AS (PARTITION BY warehouse_city), w_all_by_warehouse_opened_on AS (ORDER BY warehouse_opened_on, id)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    unit_price,
    warehouse_city,
    warehouse_capacity,
    warehouse_capacity_max_1_prior,
    unit_price_max_3_prior,
    days_since_prev_warehouse_opened_on,
    warehouse_city_min_warehouse_capacity,
    next2_warehouse_opened_on,
    warehouse_capacity_share_of_warehouse_city_wareh,
    last_known_warehouse_opened_on,
    unit_price_rank_2,
    warehouse_city_warehouse_id_sum_warehouse_capaci,
    next_unit_price,
    minutes_since_prev_moved_at,
    COALESCE(warehouse_capacity, 0) AS warehouse_capacity_filled
FROM metrics
ORDER BY warehouse_capacity_max_1_prior, moved_at NULLS LAST
