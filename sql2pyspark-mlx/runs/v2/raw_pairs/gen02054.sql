SELECT city, unique_countrys, total_count
FROM (
    SELECT city, COUNT(DISTINCT rating) AS unique_countrys, COUNT(*) AS total_count
    FROM suppliers
    WHERE supplier_name LIKE 'Gl%'
    GROUP BY city
) AS s
WHERE unique_countrys > 9;
