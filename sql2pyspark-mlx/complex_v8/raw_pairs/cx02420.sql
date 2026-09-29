WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix_2
    FROM stock_moves
),
screened AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.capacity < 11506
              AND LOWER(w.wh_code) LIKE '%nx%'
        )
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE qty > (SELECT AVG(qty) FROM screened)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        move_type_prefix,
        qty_bucket,
        COALESCE(qty_bucket, 0) AS qty_bucket_filled,
        ABS(qty_bucket - 325) AS qty_bucket_distance,
        FLOOR(qty / 100) * 100 AS qty_bucket_2
    FROM filtered
    WHERE move_type NOT IN ('OUTBOUND', 'TRANSFER')
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT s.*, w.opened_on AS warehouse_opened_on
    FROM labeled s
    JOIN warehouses w ON w.id = s.warehouse_id
),
latest_per_key AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        move_type_prefix,
        qty_bucket,
        qty_bucket_filled,
        qty_bucket_distance,
        qty_bucket_2,
        warehouse_opened_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY qty_bucket DESC, id DESC) AS recency_rank
        FROM combined_details_warehouse
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    qty,
    operator,
    move_type_prefix,
    qty_bucket_filled,
    qty_bucket_distance,
    qty_bucket_2,
    CONCAT_WS(': ', move_type_prefix, operator) AS move_type_prefix_operator_label
FROM latest_per_key
WHERE qty_bucket <= 300
ORDER BY qty_bucket_distance DESC NULLS FIRST
