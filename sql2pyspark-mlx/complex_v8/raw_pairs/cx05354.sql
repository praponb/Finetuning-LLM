WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
combined_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.unit_price_filled,
        s.new_session_flag,
        s.burst_id,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active
    FROM sessionized s
    JOIN skus sk ON sk.id = s.sku_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        new_session_flag,
        sku_is_active,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        PERCENTILE_APPROX(unit_price_filled, 0.5) AS median_unit_price_filled,
        ROUND(AVG(sku_weight_kg), 2) AS avg_sku_weight_kg,
        ROUND(STDDEV(sku_weight_kg), 2) AS stddev_sku_weight_kg
    FROM combined_details_sku
    GROUP BY new_session_flag, sku_is_active
)
-- Final projection
SELECT
    new_session_flag,
    sku_is_active,
    move_type_list,
    median_unit_price_filled,
    avg_sku_weight_kg,
    ROUND(stddev_sku_weight_kg * 1.1, 2) AS stddev_sku_weight_kg_adj,
    CASE WHEN sku_is_active THEN 1 ELSE 0 END AS sku_is_active_flag
FROM per_group
WHERE stddev_sku_weight_kg > 10.0
ORDER BY median_unit_price_filled, stddev_sku_weight_kg_adj DESC
