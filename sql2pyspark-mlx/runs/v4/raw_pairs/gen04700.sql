SELECT city, unique_citys
FROM (
    SELECT city, COUNT(DISTINCT rating) AS unique_citys
    FROM suppliers
    WHERE supplier_name LIKE '%Siam%'
    GROUP BY city
) AS t
WHERE unique_citys >= 8
ORDER BY city
