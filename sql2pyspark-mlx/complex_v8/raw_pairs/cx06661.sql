WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM stock_moves
    WHERE move_type NOT IN ('TRANSFER', 'OUTBOUND')
      AND MONTH(moved_at) IN (1, 2)
),
latest AS (
    -- Keep the latest row per move_type, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        days_since_moved_at,
        moved_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type, sku_id ORDER BY unit_price DESC, id DESC) AS row_num
        FROM scoped_moves
    ) ranked
    WHERE row_num = 1
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.days_since_moved_at,
        s.moved_at_label,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty
    FROM latest s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.move_id,
        s.move_warehouse_id,
        s.move_qty,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg
    FROM exploded_detail_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type IS NOT NULL
        )
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(sku_category), ' ', '_')) AS sku_category_slug
    FROM without_matches
)
-- Final projection
SELECT
    id,
    warehouse_id,
    qty,
    move_id,
    move_warehouse_id,
    sku_weight_kg,
    sku_category_slug,
    FLOOR(move_qty / 100) * 100 AS move_qty_bucket
FROM tagged
ORDER BY warehouse_id NULLS LAST
