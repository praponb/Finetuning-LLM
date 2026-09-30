SELECT
    id,
    country,
    city,
    UPPER(country) AS country_upper
FROM suppliers
WHERE UPPER(TRIM(supplier_name)) = 'GLOBAL SUPPLY' AND (country NOT IN ('TH', 'UK') OR country IN ('SG', 'DE', 'US'))
