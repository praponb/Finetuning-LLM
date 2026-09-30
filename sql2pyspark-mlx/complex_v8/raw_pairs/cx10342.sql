WITH valid AS (
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
        LOWER(TRIM(operator)) AS operator_clean,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND qty < 392
),
labeled AS (
    -- Add calculated columns
    SELECT *, CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref
    FROM valid
    WHERE qty < 387
),
enriched AS (
    -- Derive helper columns
    SELECT *, LOWER(TRIM(move_type_ref)) AS move_type_ref_clean
    FROM labeled
),
enriched_2 AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        s.operator_clean,
        s.move_type_ref,
        s.move_type_ref_clean,
        w.wh_code AS warehouse_wh_code,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM enriched s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
enriched_3 AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.operator_clean,
        s.move_type_ref,
        s.move_type_ref_clean,
        s.sku_category,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code_2,
        w.city AS warehouse_city
    FROM enriched_2 s
    JOIN warehouses w ON w.id = s.warehouse_id
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.operator_clean,
        s.move_type_ref,
        s.move_type_ref_clean,
        s.warehouse_capacity,
        s.warehouse_wh_code_2,
        s.warehouse_city,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id
    FROM enriched_3 s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
labeled_2 AS (
    -- Derive helper columns
    SELECT *, COALESCE(warehouse_capacity, 0) AS warehouse_capacity_filled
    FROM detail_rows
)
SELECT
    id,
    sku_id,
    unit_price,
    operator_clean,
    move_type_ref,
    move_type_ref_clean,
    warehouse_capacity,
    warehouse_wh_code_2,
    move_moved_at,
    move_sku_id,
    warehouse_capacity_filled,
    GREATEST(unit_price, warehouse_capacity) AS greatest_unit_price_warehouse_capacity,
    LOWER(REGEXP_REPLACE(TRIM(move_operator), ' ', '_')) AS move_operator_slug
FROM labeled_2
