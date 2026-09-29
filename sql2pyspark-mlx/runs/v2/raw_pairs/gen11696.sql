SELECT
    p.id,
    p.supplier_id,
    p.brand,
    p.stock_qty
FROM products p
WHERE ARRAY_CONTAINS(p.tags, 'sale') AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country = 'SG');
