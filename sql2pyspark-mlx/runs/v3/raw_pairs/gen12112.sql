SELECT city, 'supplier' AS source
FROM suppliers
WHERE LOWER(TRIM(supplier_name)) = 'pacific wholesale'
UNION
SELECT city, 'store' AS source
FROM stores;
