WITH prepared_moves AS (
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
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        moved_at_week,
        days_since_moved_at,
        unit_price_filled,
        LEAD(qty) OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price_filled ASC, id) AS next_qty,
        ROUND(AVG(unit_price_filled) OVER (PARTITION BY move_type), 2) AS move_type_avg_unit_price_filled,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price_filled ASC), 3) AS unit_price_filled_percent_rank,
        RANK() OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price_filled ASC, id) AS unit_price_filled_rank,
        SUM(unit_price) OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price_filled ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unit_price_sum_2
    FROM prepared_moves
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT s.*, sk.labels AS sku_labels
    FROM sequenced s
    JOIN skus sk ON sk.id = s.sku_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(unit_price_sum_2 * 1.1, 2) AS unit_price_sum_2_adj,
        days_since_moved_at - unit_price_filled AS days_since_moved_at_minus_unit_price_filled
    FROM with_details_sku
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    unit_price,
    moved_at_week,
    days_since_moved_at,
    unit_price_filled,
    move_type_avg_unit_price_filled,
    unit_price_filled_percent_rank,
    unit_price_sum_2,
    sku_labels,
    unit_price_sum_2_adj,
    days_since_moved_at_minus_unit_price_filled,
    DATE_FORMAT(moved_at_week, 'yyyy-MM') AS moved_at_week_label,
    DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at_2
FROM labeled
ORDER BY unit_price_filled
