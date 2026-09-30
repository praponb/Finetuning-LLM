SELECT
    ss.product_id,
    p.cost,
    p.price,
    s.city
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE NOT (p.product_name LIKE 'Noi%' AND s.supplier_name LIKE 'E%')
ORDER BY 2;
