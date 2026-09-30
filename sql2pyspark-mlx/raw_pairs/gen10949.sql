SELECT country
FROM suppliers
EXCEPT
SELECT country
FROM suppliers
WHERE country IN ('TH', 'SG', 'JP')
