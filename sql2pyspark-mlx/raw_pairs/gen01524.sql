SELECT
    ss.store_id,
    ss.product_id,
    s.store_name,
    s.city,
    p.launch_date,
    p.stock_qty
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
ORDER BY 2;
