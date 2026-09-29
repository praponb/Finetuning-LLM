SELECT
    ss.id,
    ss.store_id,
    p.stock_qty,
    p.product_name,
    s.city
FROM store_sales ss
LEFT OUTER JOIN products p ON p.id = ss.product_id
LEFT OUTER JOIN stores s ON ss.store_id = s.id;
