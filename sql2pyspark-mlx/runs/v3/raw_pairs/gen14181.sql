SELECT
    id,
    age,
    country,
    city,
    LENGTH(country) AS country_length
FROM customers
WHERE (email IS NOT NULL AND is_active = FALSE) OR country = 'TH'
ORDER BY country DESC
