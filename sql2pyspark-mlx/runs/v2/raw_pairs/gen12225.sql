SELECT
    ss.id,
    ss.product_id,
    ss.store_id,
    s.region,
    p.supplier_id,
    p.stock_qty
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
JOIN products p ON p.id = ss.product_id
WHERE p.category = 'Home' AND ss.store_id IN (3, 6)
