SELECT country, city
FROM suppliers
WHERE rating BETWEEN 3.5 AND 4.0
UNION
SELECT country, city
FROM suppliers
WHERE supplier_name LIKE '%Globa%'
ORDER BY country;
