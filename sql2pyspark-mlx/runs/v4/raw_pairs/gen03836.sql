SELECT
    p.id,
    p.cost,
    p.price,
    p.product_name,
    s.supplier_name,
    s.rating,
    s.country
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
