WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        region,
        opened_date,
        CONCAT_WS('-', city, region) AS city_region_key,
        INITCAP(TRIM(region)) AS region_title,
        COALESCE(region, 'NONE') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2024-02-27'
)
SELECT city, region
FROM valid
EXCEPT
SELECT city, region
FROM stores
WHERE region = 'Central';
