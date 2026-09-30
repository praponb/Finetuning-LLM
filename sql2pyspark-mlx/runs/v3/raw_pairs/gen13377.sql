SELECT
    rating,
    city,
    country,
    INITCAP(TRIM(country)) AS country_title
FROM suppliers
WHERE country IN ('DE', 'TH', 'SG')
