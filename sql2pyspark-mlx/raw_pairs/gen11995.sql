SELECT
    id,
    supplier_name,
    city,
    CASE WHEN country = 'US' THEN 1 ELSE 0 END AS is_us,
    SUBSTRING(country, 1, 4) AS country_prefix
FROM suppliers
WHERE rating > 2.4 AND city = 'Chiang Mai' AND supplier_name NOT LIKE '%Par%'
ORDER BY 2 DESC, city, id
LIMIT 20;
