SELECT id, country, city
FROM suppliers
WHERE supplier_name LIKE '%Parts%'
EXCEPT
SELECT id, country, city
FROM suppliers
WHERE country = 'UK'
