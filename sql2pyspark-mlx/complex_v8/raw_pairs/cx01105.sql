WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE unit_price BETWEEN 86.3 AND 436.9
      AND qty <= 288
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM base_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id >= 4
              AND st.unit_price BETWEEN 95.8 AND 330.9
        )
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM filtered s
),
unique_rows AS (
    -- Keep the latest row per sku_id, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        operator_len,
        last_moved_at,
        move_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, warehouse_id ORDER BY move_count DESC, id DESC) AS row_num
        FROM summarized_rows
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    moved_at,
    unit_price,
    operator_len,
    last_moved_at,
    move_count,
    DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
FROM unique_rows
WHERE MONTH(last_moved_at) IN (2, 3)
