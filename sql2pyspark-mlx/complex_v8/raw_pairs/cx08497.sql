WITH recent_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CASE WHEN city IN ('Bangkok', 'Singapore') THEN 'group_a' WHEN city = 'Kuala Lumpur' THEN 'group_b' ELSE 'other' END AS city_group,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug,
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_label
    FROM warehouses
    WHERE (opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-12-17' OR wh_code IS NOT NULL)
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.city,
        s.city_group,
        s.wh_code_slug,
        s.opened_on_label,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price
    FROM recent_warehouses s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
joined AS (
    -- Attach move attributes
    SELECT
        s.id,
        s.city,
        s.city_group,
        s.wh_code_slug,
        s.opened_on_label,
        s.move_id,
        s.move_moved_at,
        s.move_unit_price,
        st.sku_id AS move_sku_id,
        st.operator AS move_operator,
        st.warehouse_id AS move_warehouse_id,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity
    FROM line_level s
    JOIN stock_moves st ON st.id = s.move_id
    LEFT JOIN warehouses w ON w.id = st.warehouse_id
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM joined
    WHERE move_unit_price > (SELECT MIN(move_unit_price) FROM joined)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        city,
        wh_code_slug,
        move_id,
        move_moved_at,
        move_unit_price,
        move_operator,
        move_warehouse_id,
        warehouse_wh_code,
        warehouse_capacity,
        DATE_FORMAT(move_moved_at, 'yyyy-MM-dd HH:00') AS move_moved_at_label,
        DATEDIFF(DATE '2025-12-31', move_moved_at) AS days_since_move_moved_at,
        CASE WHEN city IN ('Kuala Lumpur', 'Singapore') THEN 'group_a' WHEN city = 'Bangkok' THEN 'group_b' ELSE 'other' END AS city_group_2
    FROM with_activity
    WHERE warehouse_capacity < 11768
)
SELECT *
FROM (
    SELECT move_id, city_group_2, warehouse_capacity
    FROM scored
) src
PIVOT (
    SUM(warehouse_capacity)
    FOR city_group_2 IN ('other', 'group_b', 'group_a')
)
ORDER BY move_id
