SELECT city, supplier_name
FROM suppliers
WHERE country <> 'US'
INTERSECT
SELECT city, supplier_name
FROM suppliers
WHERE supplier_name NOT LIKE '%Trader%'
ORDER BY city DESC;
