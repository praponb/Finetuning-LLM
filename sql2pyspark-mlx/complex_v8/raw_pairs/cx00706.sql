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
        CASE WHEN qty >= 415 THEN 'HIGH' WHEN qty >= 116 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier
    FROM stock_moves
    WHERE operator = 'op_bee'
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_details AS (
    -- Attach sku attributes
    SELECT s.*, sk.is_active AS sku_is_active, w.opened_on AS warehouse_opened_on
    FROM sessionized s
    JOIN skus sk ON sk.id = s.sku_id AND sk.weight_kg > 8.5
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE NOT sk.is_active
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        qty,
        moved_at,
        operator,
        qty_tier,
        prev_moved_at,
        starts_session,
        sku_is_active,
        warehouse_opened_on,
        CASE WHEN starts_session >= 1 THEN 'HIGH' WHEN starts_session >= 0 THEN 'MEDIUM' ELSE 'LOW' END AS starts_session_tier,
        TO_DATE(prev_moved_at) AS prev_moved_at_date,
        DATE_FORMAT(prev_moved_at, 'yyyy-MM-dd HH:00') AS prev_moved_at_label
    FROM with_details
    WHERE sku_id <= 4
)
-- Final projection
SELECT
    id,
    qty,
    moved_at,
    operator,
    qty_tier,
    prev_moved_at,
    sku_is_active,
    warehouse_opened_on,
    starts_session_tier,
    prev_moved_at_date,
    prev_moved_at_label,
    DATE_TRUNC('MONTH', warehouse_opened_on) AS warehouse_opened_on_month,
    UPPER(TRIM(operator)) AS operator_clean
FROM scored
ORDER BY qty_tier DESC
