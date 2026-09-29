SELECT supplier_name
FROM suppliers
WHERE country = 'UK'
UNION
SELECT method
FROM payments
ORDER BY supplier_name;
