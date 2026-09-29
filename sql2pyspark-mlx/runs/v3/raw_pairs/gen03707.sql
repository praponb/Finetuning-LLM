SELECT city
FROM suppliers
WHERE LENGTH(supplier_name) > 5
INTERSECT
SELECT city
FROM stores;
