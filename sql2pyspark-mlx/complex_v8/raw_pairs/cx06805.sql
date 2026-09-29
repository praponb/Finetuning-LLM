WITH base_moves AS (
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
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj
    FROM stock_moves
    WHERE unit_price <= 299.1
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM base_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator <> 'op_ann'
              AND HOUR(st.moved_at) BETWEEN 10 AND 15
        )
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        move_type_prefix,
        FLOOR(unit_price_adj / 100) * 100 AS unit_price_adj_bucket,
        FLOOR(unit_price_adj / 100) * 100 AS unit_price_adj_bucket_2
    FROM with_activity
    WHERE move_type_prefix IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(move_type)) AS move_type_clean,
        FLOOR(unit_price_adj_bucket / 100) * 100 AS unit_price_adj_bucket_bucket
    FROM tagged
),
per_group AS (
    -- Summarise per group
    SELECT
        sku_id,
        move_type,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        ROUND(AVG(unit_price_adj_bucket_bucket), 2) AS avg_unit_price_adj_bucket_bucket,
        MIN(moved_at) AS first_moved_at,
        COUNT(*) AS row_count
    FROM derived
    GROUP BY sku_id, move_type, DATE_TRUNC('MONTH', moved_at)
    HAVING SUM(unit_price_adj_bucket_bucket) > 226
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT row_count, move_type, avg_unit_price_adj_bucket_bucket
    FROM per_group
) src
PIVOT (
    SUM(avg_unit_price_adj_bucket_bucket)
    FOR move_type IN ('INBOUND', 'OUTBOUND', 'TRANSFER', 'ADJUST')
)
