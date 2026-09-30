SELECT
    id,
    rating,
    city,
    TRIM(supplier_name) AS clean_supplier_name,
    LOWER(country) AS country_lower
FROM suppliers
WHERE rating >= 2.4 OR country NOT IN ('US', 'JP')
ORDER BY rating DESC, country_lower
