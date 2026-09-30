SELECT
    ss.units,
    ss.product_id,
    p.cost,
    p.brand,
    s.rating,
    s.supplier_name,
    UPPER(TRIM(p.product_name)) AS product_name_normalized
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE ss.sale_date < '2023-05-01' AND s.country != 'US'
