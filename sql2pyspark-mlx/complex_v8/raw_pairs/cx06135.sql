WITH valid AS (
    -- Filter warehouses
    SELECT
        id,
        city,
        capacity,
        opened_on,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug
    FROM warehouses
    WHERE wh_code IS NOT NULL
      AND opened_on IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_label,
        CASE WHEN capacity >= 11506 THEN 'large' WHEN capacity >= 11005 THEN 'medium' ELSE 'small' END AS capacity_tier,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown
    FROM valid
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE capacity > (SELECT AVG(capacity) FROM derived)
)
SELECT *
FROM (
    SELECT capacity_tier, city_or_unknown, capacity
    FROM screened
) src
PIVOT (
    MIN(capacity)
    FOR city_or_unknown IN ('Bangkok', 'UNKNOWN', 'Phuket')
)
ORDER BY capacity_tier
