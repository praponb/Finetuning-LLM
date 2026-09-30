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
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        HOUR(moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE qty >= 154
      AND operator IN ('op_bee', 'op_eve')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, sk.labels AS sku_labels
    FROM cleaned s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.days_since_moved_at,
        s.moved_at_hour,
        s.sku_labels,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM with_parent s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.city NOT IN ('Singapore', 'Kuala Lumpur')
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM combined_details_warehouse s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.sku_id <= 7
              AND st.qty < 370
        )
      AND s.moved_at_hour >= 8
),
per_group AS (
    -- Summarise per group
    SELECT
        moved_at_hour,
        DATE_TRUNC('MONTH', warehouse_opened_on) AS warehouse_opened_on_month,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        ROUND(100 * COUNT(CASE WHEN warehouse_city NOT IN ('Kuala Lumpur', 'Bangkok') THEN 1 END) / COUNT(*), 1) AS pct_warehouse_city_not_in_kuala,
        SUM(CASE WHEN warehouse_city IN ('Bangkok', 'Singapore') THEN warehouse_capacity ELSE 0 END) AS warehouse_capacity_warehouse_city_in_bangkok_si,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        DATEDIFF(MAX(warehouse_opened_on), MIN(warehouse_opened_on)) AS warehouse_opened_on_span_days,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        SUM(unit_price) AS total_unit_price
    FROM filtered
    GROUP BY moved_at_hour, DATE_TRUNC('MONTH', warehouse_opened_on), DATE_TRUNC('MONTH', moved_at)
    HAVING MAX(days_since_moved_at) > 210
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', moved_at_month) AS days_since_moved_at_month
    FROM per_group
)
SELECT
    moved_at_hour,
    warehouse_opened_on_month,
    moved_at_month,
    pct_warehouse_city_not_in_kuala,
    distinct_sku_ids,
    warehouse_opened_on_span_days,
    distinct_warehouse_ids,
    total_unit_price,
    days_since_moved_at_month,
    DATEDIFF(DATE '2026-01-01', moved_at_month) AS days_since_moved_at_month_2,
    ABS(total_unit_price - 1650.5) AS total_unit_price_distance
FROM annotated
WHERE warehouse_capacity_warehouse_city_in_bangkok_si > 25000
ORDER BY moved_at_month DESC, distinct_sku_ids NULLS LAST
