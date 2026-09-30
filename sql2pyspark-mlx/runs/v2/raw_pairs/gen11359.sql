SELECT
    p.id,
    p.brand,
    s.city,
    SUBSTRING(p.brand, 1, 4) brand_prefix
FROM products p
LEFT OUTER JOIN suppliers s ON s.id = p.supplier_id
WHERE p.product_name LIKE 'C%'
ORDER BY brand_prefix;
