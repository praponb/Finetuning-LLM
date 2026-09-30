SELECT
    ss.units,
    p.supplier_id,
    s.city,
    s.supplier_name
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id;
