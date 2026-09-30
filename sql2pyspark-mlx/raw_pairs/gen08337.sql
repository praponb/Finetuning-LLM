SELECT
    id,
    country,
    city,
    COUNT(*) OVER (PARTITION BY country, city) AS country_count
FROM suppliers
ORDER BY id DESC, city
