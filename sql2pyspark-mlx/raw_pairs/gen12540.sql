WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN location IN ('Tokyo', 'Chiang Mai') THEN 'Tier 1' ELSE 'Other' END AS location_group,
        COALESCE(location, 'DIRECT') AS location_filled
    FROM departments
    WHERE dept_name IS NOT NULL
)
-- Keep one row per key (latest first)
SELECT
    id,
    dept_name,
    location,
    location_group
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS FIRST, id) = 1
