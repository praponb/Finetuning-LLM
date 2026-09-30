SELECT
    p.id,
    p.product_name,
    p.cost,
    s.city,
    s.supplier_name
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.price BETWEEN 450 AND 490;
