WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        rating,
        CASE WHEN rating >= 4.1 THEN 'High' WHEN rating >= 1.5 THEN 'Medium' ELSE 'Low' END AS rating_tier,
        SUBSTRING(city, 1, 2) AS city_prefix
    FROM suppliers
    WHERE city IS DISTINCT FROM 'New York'
)
SELECT country
FROM valid
EXCEPT ALL
SELECT country
FROM suppliers
WHERE rating IS NOT NULL;
