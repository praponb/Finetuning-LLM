SELECT city
FROM suppliers
WHERE supplier_name IS NOT NULL
INTERSECT
SELECT city
FROM suppliers
WHERE country IN ('UK', 'TH')
