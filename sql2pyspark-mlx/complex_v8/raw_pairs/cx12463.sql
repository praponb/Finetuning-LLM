WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-31 13:00:00'
),
with_parent AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        sk.category AS sku_category,
        sk.labels AS sku_labels
    FROM prepared s
    JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        sku_category,
        sku_labels,
        LEAD(moved_at) OVER (ORDER BY moved_at DESC, id) AS next_moved_at,
        ROUND(CUME_DIST() OVER w_sku_id_sku_category_by_moved, 3) AS moved_at_cume_dist
    FROM with_parent
    WINDOW w_sku_id_sku_category_by_moved AS (PARTITION BY sku_id, sku_category ORDER BY moved_at ASC)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_category,
        next_moved_at,
        moved_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at DESC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        ROUND(moved_at_cume_dist - AVG(moved_at_cume_dist) OVER (PARTITION BY warehouse_id), 2) AS moved_at_cume_dist_vs_warehouse_id_avg
    FROM with_trends
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE moved_at_cume_dist_vs_warehouse_id_avg > (SELECT AVG(moved_at_cume_dist_vs_warehouse_id_avg) FROM sequenced)
)
-- Final projection
SELECT
    id,
    sku_category,
    minutes_since_prev_moved_at,
    moved_at_cume_dist_vs_warehouse_id_avg,
    DATE_TRUNC('YEAR', next_moved_at) AS next_moved_at_year,
    COALESCE(moved_at_cume_dist, 0.0) AS moved_at_cume_dist_filled
FROM with_activity
WHERE sku_category IS NOT NULL
ORDER BY moved_at_cume_dist_vs_warehouse_id_avg
