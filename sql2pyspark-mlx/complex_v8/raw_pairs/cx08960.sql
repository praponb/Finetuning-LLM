WITH scoped_moves AS (
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
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj
    FROM stock_moves
    WHERE move_type <> 'ADJUST'
),
unique_rows AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_slug,
        unit_price_filled,
        unit_price_adj
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id DESC) AS rn
        FROM scoped_moves
    ) ranked
    WHERE rn = 1
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_slug,
        s.unit_price_filled,
        s.unit_price_adj,
        w.city AS warehouse_city
    FROM unique_rows s
    JOIN warehouses w ON w.id = s.warehouse_id
),
latest_per_key AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        move_type_slug,
        unit_price_filled,
        unit_price_adj,
        warehouse_city
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id DESC) AS recency_rank
        FROM with_parent
    ) ranked
    WHERE recency_rank = 1
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM latest_per_key s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IS NOT NULL
              AND st.warehouse_id IS NOT NULL
        )
),
deduped AS (
    -- Keep the latest row per warehouse_city
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        move_type_slug,
        unit_price_filled,
        unit_price_adj,
        warehouse_city
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_city ORDER BY unit_price DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    move_type,
    moved_at,
    operator,
    move_type_slug,
    unit_price_filled,
    unit_price_adj,
    warehouse_city,
    DATE_TRUNC('DAY', moved_at) AS moved_at_day
FROM deduped
ORDER BY id DESC
