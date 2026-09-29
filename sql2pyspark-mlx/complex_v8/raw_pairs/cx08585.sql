WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-18 15:00:00'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        NTILE(4) OVER (ORDER BY unit_price DESC, id) AS unit_price_quartile,
        unit_price_bucket - LAG(unit_price_bucket) OVER (ORDER BY unit_price DESC, id) AS unit_price_bucket_change,
        ROUND(unit_price_bucket / NULLIF(SUM(unit_price_bucket) OVER (PARTITION BY move_type), 0), 4) AS unit_price_bucket_share_of_move_type
    FROM valid
),
with_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.unit_price_bucket,
        s.operator_slug,
        s.rows_in_move_type,
        s.unit_price_quartile,
        s.unit_price_bucket_change,
        s.unit_price_bucket_share_of_move_type,
        w.opened_on AS warehouse_opened_on,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category
    FROM with_windows s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.opened_on >= DATE '2024-11-25'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_opened_on, sku_category, sku_unit_cost
    FROM with_details
) src
PIVOT (
    COUNT(sku_unit_cost)
    FOR sku_category IN ('Beauty', 'Food', 'Toys')
)
