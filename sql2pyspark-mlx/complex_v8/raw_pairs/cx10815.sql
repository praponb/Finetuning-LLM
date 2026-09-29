WITH filtered_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DATE_ADD(opened_on, 365) AS opened_on_plus_365d,
        CASE WHEN city IN ('Singapore', 'Bangkok') THEN 'group_a' WHEN city = 'Kuala Lumpur' THEN 'group_b' ELSE 'other' END AS city_group,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug
    FROM warehouses
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.capacity,
        s.opened_on,
        s.city_group,
        s.wh_code_slug,
        st.id AS move_id,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id,
        st.move_type
    FROM filtered_warehouses s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        city_group,
        wh_code_slug,
        move_id,
        move_type,
        COALESCE(capacity, 0) AS capacity_filled,
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month
    FROM line_level_moves
    WHERE move_sku_id >= 18
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug_2
    FROM derived
    WHERE wh_code_slug IS NOT NULL
)
SELECT
    move_type,
    city_group,
    move_id,
    GROUPING(move_type) AS is_total_move_type,
    COUNT(DISTINCT city_group) AS distinct_city_groups,
    MIN(opened_on_month) AS first_opened_on_month,
    COUNT(*) AS row_count
FROM enriched
GROUP BY CUBE(move_type, city_group, move_id)
