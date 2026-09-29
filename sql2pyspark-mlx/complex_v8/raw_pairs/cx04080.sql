WITH filtered_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE capacity < 11768
      AND UPPER(TRIM(wh_code)) LIKE 'K%'
),
annotated AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(city)) AS city_len
    FROM filtered_warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.capacity,
        s.opened_on,
        st.id AS move_id,
        st.move_type,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id,
        st.operator AS move_operator
    FROM annotated s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    move_id,
    move_sku_id,
    move_type,
    COUNT(DISTINCT move_sku_id) AS distinct_move_sku_ids,
    DATEDIFF(MAX(opened_on), MIN(opened_on)) AS opened_on_span_days,
    ROUND(100 * COUNT(CASE WHEN move_type = 'OUTBOUND' THEN 1 END) / COUNT(*), 1) AS pct_move_type_outbound
FROM exploded_detail
GROUP BY GROUPING SETS ((move_id, move_sku_id, move_type), (move_id), ())
ORDER BY move_id NULLS LAST, move_sku_id NULLS LAST, move_type NULLS LAST
