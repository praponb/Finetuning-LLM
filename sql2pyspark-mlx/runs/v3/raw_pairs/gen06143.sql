SELECT
    ss.store_id,
    p.supplier_id,
    p.launch_date,
    s.rating
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.category = 'Books' AND p.stock_qty = 372;
