SELECT
    s.rating,
    s.city,
    p.category,
    p.brand,
    UPPER(p.product_name) AS product_name_upper
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
WHERE s.supplier_name NOT LIKE '%Glo%' OR s.country <> 'SG';
