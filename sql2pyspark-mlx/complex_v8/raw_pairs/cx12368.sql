WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.prev_moved_at,
        s.session_no,
        sk.is_active AS sku_is_active,
        sk.sku_code,
        sk.category AS sku_category,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM sessionized s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.category <> 'Electronics'
    JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    move_type,
    operator,
    session_no,
    sku_is_active,
    sku_code,
    sku_category,
    warehouse_city,
    IF(sku_is_active, 'yes', 'no') AS sku_is_active_label,
    SUBSTRING(TRIM(warehouse_city), 1, 4) AS warehouse_city_prefix
FROM enriched
WHERE warehouse_city <> 'Kuala Lumpur'
