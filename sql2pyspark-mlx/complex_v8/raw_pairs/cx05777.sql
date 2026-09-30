WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE qty < 332
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        LOWER(REGEXP_REPLACE(TRIM(move_type_slug), ' ', '_')) AS move_type_slug_slug,
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year
    FROM cleaned
),
derived_2 AS (
    -- Label and score rows
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        move_type_slug,
        move_type_slug_slug,
        moved_at_year,
        ABS(qty - 314) AS qty_distance,
        LENGTH(TRIM(move_type_slug)) AS move_type_slug_len,
        CASE WHEN qty >= 355 THEN 'A' WHEN qty >= 168 THEN 'B' ELSE 'C' END AS qty_tier
    FROM derived
),
unique_rows AS (
    -- Keep the earliest row per move_type
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        move_type_slug,
        move_type_slug_slug,
        moved_at_year,
        qty_distance,
        move_type_slug_len,
        qty_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY qty_distance, id) AS row_num
        FROM derived_2
    ) ranked
    WHERE row_num = 1
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.move_type_slug_slug,
        s.moved_at_year,
        s.qty_distance,
        s.move_type_slug_len,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price
    FROM unique_rows s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.move_type,
        s.move_type_slug_slug,
        s.qty_distance,
        s.move_id,
        s.move_warehouse_id,
        s.move_moved_at,
        s.move_unit_price,
        st.id AS move_id_2,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price_2
    FROM with_children s
    JOIN stock_moves st ON st.warehouse_id = s.move_warehouse_id
)
SELECT
    id,
    sku_id,
    move_type,
    move_type_slug_slug,
    qty_distance,
    move_id,
    move_moved_at,
    move_id_2,
    move_operator,
    move_unit_price_2,
    ABS(move_unit_price_2 - 258.6) AS move_unit_price_2_distance,
    DATEDIFF(DATE '2025-06-30', move_moved_at) AS days_since_move_moved_at
FROM detail_rows_moves
ORDER BY sku_id DESC
