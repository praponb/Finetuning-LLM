SELECT
    id,
    rating,
    country,
    UPPER(TRIM(supplier_name)) AS supplier_name_normalized,
    LENGTH(country) AS country_length
FROM suppliers
WHERE rating > 3.0 OR country IN ('JP', 'SG')
ORDER BY rating DESC
