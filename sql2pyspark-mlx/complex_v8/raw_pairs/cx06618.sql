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
    WHERE warehouse_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY unit_price DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, sk.sku_code
    FROM unique_rows s
    JOIN skus sk ON sk.id = s.sku_id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
),
with_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT s.*, w.wh_code AS warehouse_wh_code
    FROM without_matches s
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    id,
    moved_at,
    unit_price,
    sku_code,
    DATE_TRUNC('WEEK', moved_at) AS moved_at_week,
    DATE_TRUNC('YEAR', moved_at) AS moved_at_year
FROM with_details_warehouse
ORDER BY id DESC
LIMIT 20
