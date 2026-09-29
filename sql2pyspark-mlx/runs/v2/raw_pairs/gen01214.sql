SELECT
    id,
    country,
    city,
    COALESCE(city, 'N/A') AS city_filled
FROM suppliers
WHERE city IN ('Tokyo', 'London', 'Bangkok') AND supplier_name IS NOT NULL;
