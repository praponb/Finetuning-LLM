SELECT city, supplier_name
FROM suppliers
WHERE UPPER(supplier_name) LIKE '%URO%'
EXCEPT
SELECT city, supplier_name
FROM suppliers
WHERE country NOT IN ('JP', 'UK', 'TH')
ORDER BY supplier_name DESC
