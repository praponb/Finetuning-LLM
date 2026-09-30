WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        CASE WHEN city IN ('Singapore', 'Bangkok') THEN 'group_a' WHEN city = 'Kuala Lumpur' THEN 'group_b' ELSE 'other' END AS city_group,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug
    FROM warehouses
    WHERE capacity < 11506
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.capacity,
        s.opened_on,
        s.city_group,
        st.id AS move_id,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at
    FROM base s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.id
)
SELECT
    COALESCE(move_operator, 'ALL') AS move_operator,
    move_id,
    COALESCE(city_group, 'ALL') AS city_group,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT id) AS distinct_ids
FROM exploded_detail_moves
GROUP BY GROUPING SETS ((move_operator, move_id, city_group), (move_operator), (move_id), ())
ORDER BY move_operator NULLS LAST, move_id NULLS LAST, city_group NULLS LAST
