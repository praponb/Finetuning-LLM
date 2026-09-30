WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty <= 387
),
with_parent AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        sk.category AS sku_category,
        sk.sku_code,
        sk.is_active AS sku_is_active
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
with_activity AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM with_parent s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.is_active = TRUE
              AND sk.unit_cost <= 251.2
        )
),
unique_rows AS (
    -- Keep the earliest row per sku_category
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        sku_category,
        sku_code,
        sku_is_active
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_category ORDER BY qty, id) AS recency_rank
        FROM with_activity
    ) ranked
    WHERE recency_rank = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        sku_category,
        sku_code,
        sku_is_active,
        NTILE(4) OVER w_sku_id_by_qty AS qty_quartile,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY unit_price, id) AS unit_price_dense_rank,
        unit_price - LAG(unit_price) OVER w_warehouse_id_by_unit_price AS unit_price_change
    FROM unique_rows
    WINDOW w_sku_id_by_qty AS (PARTITION BY sku_id ORDER BY qty, id), w_warehouse_id_by_unit_price AS (PARTITION BY warehouse_id ORDER BY unit_price, id)
)
-- Final projection
SELECT
    id,
    move_type,
    sku_category,
    sku_is_active,
    first_known_moved_at,
    unit_price_dense_rank,
    unit_price_change,
    moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d
FROM with_windows
ORDER BY move_type DESC
