WITH valid AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity
    FROM warehouses
    WHERE capacity < 11005
      AND wh_code IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        ABS(capacity - 11506) AS capacity_distance
    FROM valid
),
scored_2 AS (
    -- Add calculated columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        CASE WHEN capacity_distance >= 12000 THEN 'A' WHEN capacity_distance >= 6000 THEN 'B' ELSE 'C' END AS capacity_distance_tier,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug,
        CASE WHEN capacity >= 11005 THEN 'large' WHEN capacity >= 4912 THEN 'medium' ELSE 'small' END AS capacity_tier
    FROM scored
    WHERE capacity <= 11506
)
-- Final projection
SELECT
    id,
    wh_code,
    city,
    capacity,
    capacity_distance_tier,
    wh_code_slug,
    capacity_tier
FROM scored_2
ORDER BY city DESC NULLS FIRST, capacity DESC
