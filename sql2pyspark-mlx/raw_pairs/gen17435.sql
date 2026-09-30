SELECT
    p.id,
    p.price,
    p.product_name,
    s.country,
    s.rating
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.supplier_name LIKE '%Toky%' AND p.brand NOT IN ('Stark', 'Umbrella')
