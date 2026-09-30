WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        TO_DATE(moved_at) AS moved_at_date,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND unit_price <= 313.8
),
latest_per_key AS (
    -- Keep the latest row per sku_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        moved_at_date,
        move_type_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY qty DESC, id DESC) AS rn
        FROM base_moves
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        moved_at_date,
        DATEDIFF(DATE '2025-12-31', moved_at_date) AS days_since_moved_at_date,
        COALESCE(qty, 0) AS qty_filled
    FROM latest_per_key
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.moved_at_date,
        s.days_since_moved_at_date,
        s.qty_filled,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        sk.sku_code
    FROM scored s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN qty_filled >= 350 THEN 'large' WHEN qty_filled >= 201 THEN 'medium' ELSE 'small' END AS qty_filled_tier
    FROM enriched_warehouse
),
deduped AS (
    -- Keep the latest row per qty_filled_tier, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        moved_at_date,
        days_since_moved_at_date,
        qty_filled,
        warehouse_opened_on,
        warehouse_city,
        warehouse_capacity,
        sku_code,
        qty_filled_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY qty_filled_tier, sku_id ORDER BY warehouse_capacity DESC, id DESC) AS rn
        FROM labeled
    ) ranked
    WHERE rn = 1
),
labeled_2 AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(days_since_moved_at_date - 280) AS days_since_moved_at_date_distance,
        CONCAT(warehouse_city, '#', CAST(sku_id AS STRING)) AS warehouse_city_ref,
        warehouse_capacity + days_since_moved_at_date AS warehouse_capacity_plus_days_since_moved_at_date
    FROM deduped
    WHERE LENGTH(TRIM(sku_code)) >= 4
)
-- Final projection
SELECT
    id,
    warehouse_id,
    operator,
    moved_at_date,
    qty_filled,
    warehouse_city,
    warehouse_capacity,
    sku_code,
    qty_filled_tier,
    days_since_moved_at_date_distance,
    warehouse_city_ref,
    warehouse_capacity_plus_days_since_moved_at_date,
    SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix
FROM labeled_2
WHERE days_since_moved_at_date > 70
ORDER BY days_since_moved_at_date_distance DESC
