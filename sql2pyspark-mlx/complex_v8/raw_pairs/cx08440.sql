WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        ABS(capacity - 11506) AS capacity_distance
    FROM warehouses
    WHERE city NOT IN ('Singapore', 'Bangkok')
      AND LOWER(wh_code) LIKE '%ku%'
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared
    WHERE capacity > (SELECT AVG(capacity) FROM prepared)
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.city,
        s.capacity,
        s.opened_on,
        s.capacity_distance,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.moved_at AS move_moved_at,
        st.move_type,
        st.sku_id AS move_sku_id
    FROM screened s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.id AND MONTH(st.moved_at) IN (1, 3)
)
SELECT
    id,
    city,
    opened_on,
    move_id,
    move_unit_price,
    move_moved_at,
    move_type,
    move_sku_id
FROM exploded_detail
ORDER BY opened_on
