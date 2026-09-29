WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        WEEKOFYEAR(opened_date) AS opened_date_weekofyear,
        CONCAT_WS('-', city, region) AS city_region_key
    FROM stores
    WHERE opened_date >= DATE '2023-07-14'
)
SELECT city, id
FROM cleaned
EXCEPT
SELECT city, id
FROM stores
WHERE LENGTH(store_name) < 14
