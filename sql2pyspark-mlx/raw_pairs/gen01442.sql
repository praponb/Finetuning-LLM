SELECT
    id,
    email,
    CONCAT(country, '/', segment) AS country_segment,
    LOWER(city) AS city_lower
FROM customers
WHERE credit_limit IS NULL AND credit_limit >= 8600 AND NOT is_active;
