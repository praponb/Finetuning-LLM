WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        moved_at + INTERVAL 1 DAYS AS moved_at_plus_1d,
        LEAST(qty, unit_price) AS least_qty_unit_price,
        CASE WHEN unit_price >= 323.9 THEN 'A' WHEN unit_price >= 189.4 THEN 'B' ELSE 'C' END AS unit_price_tier
    FROM stock_moves
    WHERE operator IN ('op_eve', 'op_bee')
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM recent_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price <= 330.9
              AND st.qty < 355
        )
      AND s.warehouse_id IS NOT NULL
),
deduped AS (
    -- Keep the earliest row per sku_id, unit_price_tier
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        moved_at_plus_1d,
        least_qty_unit_price,
        unit_price_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, unit_price_tier ORDER BY moved_at, id) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM deduped s
),
deduped_2 AS (
    -- Keep the earliest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        moved_at_plus_1d,
        least_qty_unit_price,
        unit_price_tier,
        first_moved_at,
        last_moved_at,
        move_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY move_count, id) AS row_num
        FROM profiled
    ) ranked
    WHERE row_num = 1
),
enriched_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at_plus_1d,
        s.first_moved_at,
        s.last_moved_at,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg
    FROM deduped_2 s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.wh_code IS NOT NULL
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at_plus_1d,
    first_moved_at,
    last_moved_at,
    warehouse_wh_code,
    warehouse_opened_on,
    sku_code,
    FLOOR(qty / 100) * 100 AS qty_bucket,
    last_moved_at + INTERVAL 1 DAYS AS last_moved_at_plus_1d
FROM enriched_warehouse
