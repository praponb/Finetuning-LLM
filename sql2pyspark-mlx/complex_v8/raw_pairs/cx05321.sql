WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator IS NOT NULL
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        st.id AS move_id,
        st.sku_id AS move_sku_id,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price
    FROM filtered s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
combined_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.move_id,
        s.move_sku_id,
        s.move_operator,
        s.move_unit_price,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        sk.category AS sku_category
    FROM detail_rows s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty > 99
              AND st.warehouse_id >= 5
        )
      AND s.move_sku_id <= 11
),
enriched_move AS (
    -- Attach move attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.move_id,
        s.move_sku_id,
        s.move_operator,
        s.move_unit_price,
        s.warehouse_capacity,
        s.warehouse_wh_code,
        s.sku_category,
        st.qty AS move_qty,
        st.operator AS move_operator_2,
        st.sku_id AS move_sku_id_2
    FROM screened s
    JOIN stock_moves st ON st.id = s.move_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        move_sku_id_2,
        move_operator_2,
        move_id,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        MIN(moved_at) AS first_moved_at,
        COUNT(DISTINCT move_id) AS distinct_move_ids
    FROM enriched_move
    GROUP BY move_sku_id_2, move_operator_2, move_id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        first_moved_at + INTERVAL 30 MINUTES AS first_moved_at_plus_30m,
        ROUND(distinct_sku_ids / NULLIF(distinct_ids, 0), 3) AS distinct_sku_ids_per_distinct_ids
    FROM rollup_base
    WHERE HOUR(first_moved_at) BETWEEN 10 AND 12
)
-- Final projection
SELECT
    move_sku_id_2,
    move_operator_2,
    distinct_ids,
    distinct_sku_ids,
    first_moved_at,
    distinct_move_ids,
    first_moved_at_plus_30m,
    TO_DATE(first_moved_at) AS first_moved_at_date,
    DATEDIFF(DATE '2025-06-30', first_moved_at) AS days_since_first_moved_at
FROM labeled
ORDER BY first_moved_at DESC
