SELECT supplier_name, city
FROM suppliers
WHERE supplier_name LIKE '%Global%'
UNION ALL
SELECT supplier_name, city
FROM suppliers
WHERE rating BETWEEN 2.1 AND 3.2
ORDER BY city;
