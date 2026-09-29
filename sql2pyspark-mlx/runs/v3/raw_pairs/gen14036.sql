SELECT
    ss.id,
    ss.units,
    p.product_name,
    p.price,
    s.opened_date
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
WHERE s.store_name IS NOT NULL AND ss.sale_date BETWEEN '2023-11-01' AND '2023-12-01';
