WITH valid_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        city,
        capacity,
        opened_on,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug
    FROM warehouses
    WHERE opened_on >= DATE '2024-04-13'
      AND LENGTH(TRIM(wh_code)) >= 4
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(city), 1, 2) AS city_prefix,
        ADD_MONTHS(opened_on, 6) AS opened_on_plus_6m
    FROM valid_warehouses
)
-- Final projection
SELECT
    id,
    city,
    opened_on,
    city_prefix,
    opened_on_plus_6m,
    DATEDIFF(DATE '2025-12-31', opened_on) AS days_since_opened_on,
    CASE WHEN city IN ('Bangkok', 'Singapore') THEN 'group_a' WHEN city = 'Kuala Lumpur' THEN 'group_b' ELSE 'other' END AS city_group
FROM tagged
ORDER BY opened_on_plus_6m, city_prefix NULLS LAST
