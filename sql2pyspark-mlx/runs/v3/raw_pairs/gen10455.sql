SELECT
    p.id,
    p.cost,
    p.product_name,
    p.supplier_id
FROM products p
WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE supplier_name LIKE '%Tok%');
