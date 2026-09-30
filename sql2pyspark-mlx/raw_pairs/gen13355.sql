WITH valid AS (
    SELECT
        *,
        UPPER(city) AS city_upper,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date >= TO_DATE('2023-05-30')
      AND region IS NOT NULL
)
SELECT region, id
FROM valid
EXCEPT
SELECT region, id
FROM stores
WHERE region = 'South';
