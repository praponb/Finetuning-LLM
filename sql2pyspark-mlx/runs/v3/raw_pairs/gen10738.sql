SELECT
    id,
    city,
    country,
    rating
FROM suppliers
WHERE city != 'London' AND supplier_name LIKE 'P%' AND city != 'Tokyo'
ORDER BY city;
