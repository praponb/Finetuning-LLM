WITH filtered AS (
    SELECT
        id,
        city,
        opened_date,
        LOWER(region) AS region_lower
    FROM stores
    WHERE region IS NOT NULL
)
SELECT city, id
FROM filtered
EXCEPT ALL
SELECT city, id
FROM stores
WHERE sqft BETWEEN 4914 AND 15288;
