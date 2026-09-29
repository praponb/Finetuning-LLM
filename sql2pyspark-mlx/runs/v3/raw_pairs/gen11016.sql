SELECT
    p.id,
    p.category,
    p.brand,
    p.supplier_id
FROM products p
WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country <> 'JP')
