SELECT
    id,
    opened_date,
    UPPER(city) AS city_upper,
    CONCAT(region, '/', city) AS region_city
FROM stores
WHERE store_name IS NOT NULL AND MONTH(opened_date) IN (6, 7, 10)
