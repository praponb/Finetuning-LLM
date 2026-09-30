WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND move_type IN ('TRANSFER', 'ADJUST')
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.category AS sku_category,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY move_type ORDER BY days_since_moved_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_sum_7,
        LAST_VALUE(moved_at) OVER (PARTITION BY move_type ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        NTH_VALUE(days_since_moved_at, 2) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_moved_at,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator)) AS distinct_warehouse_id_in_operator,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_is_active), 0), 4) AS unit_price_share_of_sku_is_active,
        COUNT(*) OVER w_sku_is_active AS rows_in_sku_is_active,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY sku_id, YEAR(moved_at) ORDER BY moved_at_date NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS warehouse_capacity_avg_6_prior,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator,
        ROUND(AVG(warehouse_capacity) OVER w_sku_is_active, 2) AS sku_is_active_avg_warehouse_capacity
    FROM with_details_sku
    WINDOW w_sku_is_active AS (PARTITION BY sku_is_active)
),
derived AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM windowed
    WHERE rows_in_sku_id > 3
)
SELECT
    id,
    sku_id,
    move_type,
    qty,
    moved_at,
    days_since_moved_at,
    sku_category,
    sku_labels,
    warehouse_wh_code,
    warehouse_capacity,
    unit_price_sum_7,
    last_moved_at,
    second_days_since_moved_at,
    distinct_warehouse_id_in_operator,
    rows_in_sku_is_active,
    warehouse_capacity_avg_6_prior,
    rows_in_operator,
    sku_is_active_avg_warehouse_capacity,
    moved_at_label,
    COALESCE(unit_price, 0.0) AS unit_price_filled
FROM derived
ORDER BY sku_is_active_avg_warehouse_capacity DESC, id
LIMIT 20
