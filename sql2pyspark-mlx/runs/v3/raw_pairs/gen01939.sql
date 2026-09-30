SELECT
    ss.id,
    ss.sale_date,
    ss.units,
    p.product_name,
    s.city,
    s.supplier_name,
    CONCAT(s.supplier_name, ' - ', s.city) supplier_name_city
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.rating < 2.6
ORDER BY city
