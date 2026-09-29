WITH recent AS (
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
        YEAR(moved_at) AS moved_at_year,
        unit_price - qty AS unit_price_minus_qty,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE qty BETWEEN 145 AND 355
),
combined_details AS (
    -- Join warehouses
    SELECT s.*, w.wh_code AS warehouse_wh_code, w.capacity AS warehouse_capacity
    FROM recent s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.capacity BETWEEN 4912 AND 11506
),
enriched_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_minus_qty,
        s.unit_price_per_qty,
        s.warehouse_wh_code,
        s.warehouse_capacity,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM combined_details s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE SIZE(sk.labels) >= 1
),
per_group AS (
    -- Group-level totals
    SELECT
        move_type,
        sku_id,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        SUM(CASE WHEN UPPER(TRIM(warehouse_wh_code)) LIKE 'B%' THEN unit_price_per_qty ELSE 0 END) AS unit_price_per_qty_upper_trim_warehouse_wh_code
    FROM enriched_sku
    GROUP BY move_type, sku_id
    HAVING MAX(unit_price_per_qty) > 3.0
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(distinct_warehouse_ids) OVER (PARTITION BY sku_id, distinct_warehouse_ids ORDER BY unit_price_per_qty_upper_trim_warehouse_wh_code ASC, move_type, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_warehouse_ids,
        ROUND(distinct_warehouse_ids - AVG(distinct_warehouse_ids) OVER (PARTITION BY move_type), 2) AS distinct_warehouse_ids_vs_move_type_avg,
        FIRST_VALUE(distinct_warehouse_ids) IGNORE NULLS OVER (ORDER BY distinct_warehouse_ids, move_type, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_warehouse_ids,
        ROUND(AVG(distinct_warehouse_ids) OVER (PARTITION BY move_type, sku_id ORDER BY unit_price_per_qty_upper_trim_warehouse_wh_code ASC, move_type, sku_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS distinct_warehouse_ids_avg_7
    FROM per_group
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.move_type,
        s.sku_id,
        s.distinct_warehouse_ids,
        s.distinct_warehouse_ids_vs_move_type_avg,
        st.id AS move_id,
        st.qty AS move_qty,
        st.move_type AS move_move_type,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at
    FROM with_trends s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_children_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id <= 3
        )
)
SELECT
    move_type,
    sku_id,
    distinct_warehouse_ids,
    distinct_warehouse_ids_vs_move_type_avg,
    move_id,
    move_qty,
    move_operator,
    DATE_TRUNC('HOUR', move_moved_at) AS move_moved_at_hour,
    DATE_FORMAT(move_moved_at, 'MMM yyyy') AS move_moved_at_label
FROM without_matches
ORDER BY move_id DESC, move_type, sku_id
LIMIT 10
