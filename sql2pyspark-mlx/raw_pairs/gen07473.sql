SELECT supplier_name AS label
FROM suppliers
EXCEPT
SELECT category AS label
FROM products
ORDER BY label;
