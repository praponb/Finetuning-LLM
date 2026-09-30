SELECT
    suppliers.id,
    suppliers.supplier_name,
    suppliers.country,
    suppliers.rating
FROM suppliers
WHERE UPPER(suppliers.supplier_name) LIKE '%OKY%' AND suppliers.id IN (SELECT supplier_id FROM products WHERE price >= 600)
