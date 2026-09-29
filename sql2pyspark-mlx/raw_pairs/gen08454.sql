WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('/', supplier_name, city) AS supplier_name_city_key,
        FLOOR(rating) AS rating_whole,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE city = 'Tokyo'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        country,
        city,
        rating_whole
    FROM cleaned
) src
PIVOT (
    MAX(rating_whole)
    FOR city IN ('Berlin' AS berlin, 'New York' AS new_york)
);
