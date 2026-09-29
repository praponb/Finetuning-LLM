WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN unit_price >= 298.9 THEN 'large' WHEN unit_price >= 238.5 THEN 'medium' ELSE 'small' END AS unit_price_tier,
        TO_DATE(moved_at) AS moved_at_date,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM valid_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price > 238.5
              AND st.operator IN ('op_bee', 'op_chan', 'op_ann')
        )
      AND s.qty < 422
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE qty_plus_unit_price > (SELECT AVG(qty_plus_unit_price) FROM screened)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(qty_plus_unit_price * 1.5, 2) AS qty_plus_unit_price_adj,
        CONCAT(unit_price_tier, '#', CAST(qty AS STRING)) AS unit_price_tier_ref,
        ROUND(unit_price / NULLIF(qty_plus_unit_price, 0), 3) AS unit_price_per_qty_plus_unit_price
    FROM filtered
    WHERE warehouse_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(unit_price) OVER (PARTITION BY sku_id) AS sku_id_max_unit_price,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id
    FROM scored
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price,
    unit_price_tier,
    qty_plus_unit_price,
    qty_plus_unit_price_adj,
    unit_price_per_qty_plus_unit_price,
    sku_id_max_unit_price,
    rows_in_sku_id,
    LOWER(TRIM(unit_price_tier_ref)) AS unit_price_tier_ref_clean,
    DATE_ADD(moved_at_date, 90) AS moved_at_date_plus_90d
FROM with_windows
ORDER BY unit_price_tier_ref_clean DESC
