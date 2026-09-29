SELECT supplier_id
FROM products
INTERSECT
SELECT supplier_id
FROM products
WHERE brand NOT IN ('Initech', 'Acme', 'Globex');
