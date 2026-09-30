SELECT
    p.id,
    p.price,
    p.launch_date,
    p.stock_qty
FROM products p
WHERE ARRAY_CONTAINS(p.tags, 'eco') AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country NOT IN ('DE', 'US'));
