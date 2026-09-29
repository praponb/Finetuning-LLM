SELECT city
FROM suppliers
WHERE supplier_name IS NOT NULL
UNION ALL
SELECT city
FROM suppliers
WHERE rating > 2.4
ORDER BY city DESC
