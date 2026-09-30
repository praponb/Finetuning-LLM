SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    p.supplier_id,
    p.product_name,
    s.city
FROM store_sales ss
FULL OUTER JOIN products p ON p.id = ss.product_id
INNER JOIN stores s ON ss.store_id = s.id
