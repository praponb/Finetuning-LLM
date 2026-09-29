WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        ROUND(unit_price * 1.1, 2) AS unit_price_adj
    FROM stock_moves
    WHERE unit_price < 289.7
),
totals AS (
    -- Summarise per group
    SELECT
        move_type,
        DATE_TRUNC('MONTH', moved_at_date) AS moved_at_date_month,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(qty_per_unit_price) / NULLIF(SUM(unit_price), 0), 3) AS qty_per_unit_price_to_unit_price_ratio,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids
    FROM filtered
    WHERE unit_price >= 205.1
    GROUP BY move_type, DATE_TRUNC('MONTH', moved_at_date)
    HAVING MAX(unit_price_adj) > 263.1
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(move_type)) AS move_type_clean,
        CONCAT(move_type, '#', CAST(distinct_ids AS STRING)) AS move_type_ref,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM totals
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM enriched
    WHERE distinct_warehouse_ids < (SELECT MAX(distinct_warehouse_ids) FROM enriched)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM screened)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE qty_per_unit_price_to_unit_price_ratio > (SELECT AVG(qty_per_unit_price_to_unit_price_ratio) FROM with_activity)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_warehouse_ids, move_type, distinct_ids
    FROM qualified
) src
PIVOT (
    COUNT(distinct_ids)
    FOR move_type IN ('TRANSFER', 'OUTBOUND')
)
ORDER BY distinct_warehouse_ids
