SELECT
    p.id,
    p.supplier_id,
    p.product_name,
    s.supplier_name,
    s.country,
    s.rating,
    INITCAP(TRIM(p.brand)) AS brand_title
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
