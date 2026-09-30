WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE qty < 271
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        move_type,
        qty,
        unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label,
        ABS(qty - 154) AS qty_distance
    FROM without_matches
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        move_type,
        qty,
        unit_price,
        moved_at_label,
        qty_distance,
        LOWER(TRIM(moved_at_label)) AS moved_at_label_clean,
        GREATEST(qty_distance, qty) AS greatest_qty_distance_qty
    FROM labeled
    WHERE qty_distance <= 425
),
stats AS (
    -- Group-level totals
    SELECT
        move_type,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        COUNT(*) FILTER (WHERE move_type = 'INBOUND') AS n_move_type_inbound,
        ROUND(SUM(qty) / NULLIF(SUM(unit_price), 0), 3) AS qty_to_unit_price_ratio,
        SUM(CASE WHEN move_type <> 'OUTBOUND' THEN qty ELSE 0 END) AS qty_move_type_outbound
    FROM annotated
    GROUP BY move_type
)
SELECT
    n_move_type_inbound,
    move_type,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list_2,
    ROUND(100 * COUNT(CASE WHEN move_type = 'TRANSFER' THEN 1 END) / COUNT(*), 1) AS pct_move_type_transfer
FROM stats
GROUP BY ROLLUP(n_move_type_inbound, move_type)
ORDER BY n_move_type_inbound NULLS LAST, move_type NULLS LAST
