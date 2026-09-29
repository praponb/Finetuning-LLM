WITH valid_moves AS (
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
        CASE WHEN qty >= 392 THEN 'A' WHEN qty >= 148 THEN 'B' ELSE 'C' END AS qty_tier
    FROM stock_moves
    WHERE qty > 137
      AND unit_price BETWEEN 60.4 AND 317.0
),
with_parent_warehouse AS (
    -- Join warehouses
    SELECT
        s.*,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM valid_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        qty_tier,
        warehouse_wh_code,
        warehouse_city,
        warehouse_opened_on,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_id), 2) AS warehouse_id_sku_id_avg_unit_price,
        LAST_VALUE(warehouse_opened_on) OVER (PARTITION BY sku_id ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_warehouse_opened_on,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY qty_tier, YEAR(warehouse_opened_on) ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        NTILE(5) OVER (PARTITION BY sku_id ORDER BY warehouse_opened_on DESC, id) AS warehouse_opened_on_quintile,
        COUNT(*) OVER (PARTITION BY warehouse_id, sku_id) AS rows_in_warehouse_id_sku_id,
        SIZE(COLLECT_SET(qty_tier) OVER w_warehouse_id_sku_id) AS distinct_qty_tier_in_warehouse_id_sku_id,
        NTH_VALUE(qty, 2) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        COUNT(*) OVER (PARTITION BY warehouse_city, move_type) AS rows_in_warehouse_city_move_type,
        ROUND(qty / NULLIF(SUM(qty) OVER w_warehouse_city_move_type, 0), 4) AS qty_share_of_warehouse_city_move_type,
        DENSE_RANK() OVER (PARTITION BY qty_tier, warehouse_city ORDER BY warehouse_opened_on DESC) AS warehouse_opened_on_dense_rank,
        MAX(qty) OVER (PARTITION BY warehouse_city, move_type) AS warehouse_city_move_type_max_qty,
        NTH_VALUE(warehouse_opened_on, 2) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        ROUND(PERCENT_RANK() OVER w_qty_tier_warehouse_opened_on_2, 3) AS unit_price_percent_rank,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY qty_tier, warehouse_city ORDER BY warehouse_opened_on DESC, id))) / 60, 1) AS minutes_since_prev_moved_at
    FROM with_parent_warehouse
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id), w_warehouse_city_move_type AS (PARTITION BY warehouse_city, move_type), w_qty_tier_warehouse_opened_on_2 AS (PARTITION BY qty_tier, YEAR(warehouse_opened_on) ORDER BY unit_price DESC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(minutes_since_prev_moved_at) OVER (PARTITION BY warehouse_city, warehouse_opened_on_quintile) AS warehouse_city_warehouse_opened_on_quintile_sum_,
        COUNT(*) OVER (PARTITION BY qty_tier, YEAR(moved_at) ORDER BY rows_in_warehouse_id_sku_id ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER (PARTITION BY warehouse_city, warehouse_opened_on_quintile) AS rows_in_warehouse_city_warehouse_opened_on_quint,
        MIN(warehouse_opened_on_quintile) OVER (PARTITION BY warehouse_city, warehouse_opened_on_quintile) AS warehouse_city_warehouse_opened_on_quintile_min_
    FROM with_trends
)
SELECT
    id,
    moved_at,
    unit_price,
    qty_tier,
    warehouse_wh_code,
    last_warehouse_opened_on,
    first_known_moved_at,
    rows_in_warehouse_id_sku_id,
    second_qty,
    rows_in_warehouse_city_move_type,
    qty_share_of_warehouse_city_move_type,
    warehouse_opened_on_dense_rank,
    second_warehouse_opened_on,
    unit_price_percent_rank,
    minutes_since_prev_moved_at,
    warehouse_city_warehouse_opened_on_quintile_sum_,
    running_count,
    rows_in_warehouse_city_warehouse_opened_on_quint,
    warehouse_city_warehouse_opened_on_quintile_min_
FROM metrics
WHERE rows_in_warehouse_id_sku_id BETWEEN 5 AND 15
ORDER BY warehouse_city_warehouse_opened_on_quintile_min_ DESC
