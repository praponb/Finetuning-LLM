SELECT
    p.id,
    p.product_name,
    s.city,
    s.country,
    s.supplier_name
FROM products p
RIGHT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY s.country, p.id DESC
