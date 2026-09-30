WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        INITCAP(TRIM(operator)) AS operator_clean,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        HOUR(moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
),
deduped AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        operator_clean,
        qty_bucket,
        moved_at_hour
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty_bucket DESC, id DESC) AS recency_rank
        FROM prepared_moves
    ) ranked
    WHERE recency_rank = 1
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.city AS warehouse_city,
        sk.labels AS sku_labels,
        sk.sku_code
    FROM deduped s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        moved_at_hour,
        move_type,
        operator,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        SUM(moved_at_hour) AS total_moved_at_hour,
        COUNT(*) AS n_rows,
        ROUND(AVG(qty_bucket), 2) AS avg_qty_bucket,
        ROUND(SUM(moved_at_hour) / NULLIF(SUM(qty), 0), 3) AS moved_at_hour_to_qty_ratio
    FROM with_details_warehouse
    GROUP BY 1, 2, 3
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE n_rows > (SELECT AVG(n_rows) FROM totals)
),
derived AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(operator)) AS operator_len
    FROM screened
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_rows, moved_at_hour, move_type, moved_at_hour_to_qty_ratio
    FROM derived
) src
PIVOT (
    AVG(moved_at_hour_to_qty_ratio) AS avg_v, COUNT(moved_at_hour_to_qty_ratio) AS n
    FOR move_type IN ('ADJUST', 'INBOUND', 'TRANSFER', 'OUTBOUND')
)
