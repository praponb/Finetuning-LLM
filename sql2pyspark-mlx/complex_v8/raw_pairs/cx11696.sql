WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
with_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity,
        sk.labels AS sku_labels
    FROM filtered s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE UPPER(TRIM(w.wh_code)) LIKE 'B%'
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        warehouse_city,
        warehouse_capacity,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM with_details
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        moved_at,
        operator,
        unit_price,
        SUM(unit_price) OVER (PARTITION BY operator, warehouse_city) AS operator_warehouse_city_sum_unit_price,
        ROW_NUMBER() OVER (ORDER BY moved_at, id) AS rn,
        LAG(unit_price) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_unit_price,
        MAX(unit_price) OVER (PARTITION BY operator, warehouse_city) AS operator_warehouse_city_max_unit_price,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at), 3) AS moved_at_cume_dist,
        NTILE(5) OVER w_all_by_moved_at AS moved_at_quintile
    FROM enriched
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at, id)
),
totals AS (
    -- Group-level totals
    SELECT
        moved_at_quintile,
        ROUND(SUM(prev_unit_price) / NULLIF(SUM(moved_at_quintile), 0), 3) AS prev_unit_price_to_moved_at_quintile_ratio,
        ROUND(AVG(rn), 2) AS avg_rn,
        ROUND(AVG(moved_at_quintile), 2) AS avg_moved_at_quintile
    FROM windowed
    GROUP BY moved_at_quintile
)
-- Final projection
SELECT
    moved_at_quintile,
    prev_unit_price_to_moved_at_quintile_ratio,
    avg_moved_at_quintile
FROM totals
ORDER BY avg_moved_at_quintile DESC, prev_unit_price_to_moved_at_quintile_ratio DESC
