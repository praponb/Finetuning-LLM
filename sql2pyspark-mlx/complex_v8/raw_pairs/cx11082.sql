WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        unit_price + qty AS unit_price_plus_qty,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE unit_price >= 86.3
),
move_type_or_unknown_stats AS (
    -- Median moved_at_dayofweek per move_type_or_unknown
    SELECT move_type_or_unknown, PERCENTILE_APPROX(moved_at_dayofweek, 0.5) AS median_moved_at_dayofweek
    FROM prepared
    GROUP BY move_type_or_unknown
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_moved_at_dayofweek,
        ROUND(s.moved_at_dayofweek - g.median_moved_at_dayofweek, 2) AS moved_at_dayofweek_minus_median
    FROM prepared s
    JOIN move_type_or_unknown_stats g ON g.move_type_or_unknown = s.move_type_or_unknown
    WHERE s.moved_at_dayofweek >= g.median_moved_at_dayofweek
),
joined AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.move_type_or_unknown,
        s.unit_price_plus_qty,
        s.moved_at_dayofweek,
        s.median_moved_at_dayofweek,
        s.moved_at_dayofweek_minus_median,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost
    FROM above_average s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.unit_cost BETWEEN 134.3 AND 302.5
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at_dayofweek,
    median_moved_at_dayofweek,
    moved_at_dayofweek_minus_median,
    sku_is_active,
    sku_unit_cost,
    CASE WHEN unit_price_plus_qty >= 800.2 THEN 'A' WHEN unit_price_plus_qty >= 400.6 THEN 'B' ELSE 'C' END AS unit_price_plus_qty_tier,
    ROUND(unit_price_plus_qty / NULLIF(sku_unit_cost, 0), 3) AS unit_price_plus_qty_per_sku_unit_cost
FROM joined
ORDER BY moved_at_dayofweek_minus_median DESC
