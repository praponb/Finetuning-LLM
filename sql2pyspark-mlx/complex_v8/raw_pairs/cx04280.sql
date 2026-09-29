WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE sku_id >= 4
),
deduped AS (
    -- Keep the latest row per operator, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_len,
        days_since_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY moved_at DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.operator_len,
        s.days_since_moved_at,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        sk.unit_cost AS sku_unit_cost
    FROM deduped s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.opened_on >= DATE '2024-11-25'
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
filtered_2 AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM enriched_warehouse s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.is_active = FALSE
        )
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.operator_len,
        s.days_since_moved_at,
        s.warehouse_city,
        s.warehouse_capacity,
        s.sku_unit_cost,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM filtered_2 s
    JOIN warehouses w ON w.id = s.warehouse_id
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.days_since_moved_at,
        s.sku_unit_cost,
        s.warehouse_wh_code,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty,
        st.operator AS move_operator
    FROM with_parent_warehouse s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.qty <= 326
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    operator,
    days_since_moved_at,
    sku_unit_cost,
    move_id,
    move_moved_at,
    move_qty,
    move_operator
FROM exploded_detail
ORDER BY id DESC, warehouse_id
