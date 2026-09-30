SELECT
    ss.id,
    ss.sale_date,
    ss.store_id,
    p.stock_qty,
    p.category,
    s.supplier_name,
    s.country
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.launch_date >= '2024-04-24';
