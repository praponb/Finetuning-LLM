SELECT
    region,
    city,
    opened_date,
    CONCAT(city, '/', region) AS city_region
FROM stores
ORDER BY opened_date, region DESC
