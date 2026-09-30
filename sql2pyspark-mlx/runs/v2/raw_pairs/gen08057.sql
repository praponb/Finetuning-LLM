SELECT
    ss.units,
    p.supplier_id,
    p.price,
    s.store_name
FROM store_sales ss
LEFT OUTER JOIN products p ON p.id = ss.product_id
INNER JOIN stores s ON s.id = ss.store_id;
