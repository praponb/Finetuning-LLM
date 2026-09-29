SELECT
    ss.store_id,
    ss.sale_date,
    p.product_name,
    p.price,
    s.country,
    s.rating
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id;
