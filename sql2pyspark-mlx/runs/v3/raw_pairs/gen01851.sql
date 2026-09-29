SELECT supplier_name
FROM suppliers
WHERE country IN ('JP', 'SG')
EXCEPT
SELECT supplier_name
FROM suppliers
WHERE country IN ('JP', 'TH', 'US')
