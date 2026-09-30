SELECT
    ss.id,
    ss.store_id,
    ss.product_id,
    ss.units,
    p.price
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
WHERE p.stock_qty BETWEEN 281 AND 313 AND p.launch_date BETWEEN '2023-04-01' AND '2023-07-14';
