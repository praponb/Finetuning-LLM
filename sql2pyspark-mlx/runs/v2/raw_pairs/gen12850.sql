SELECT
    i.id,
    i.quantity,
    s.region,
    p.price,
    p.stock_qty
FROM inventory i
LEFT JOIN stores s ON s.id = i.store_id
RIGHT JOIN products p ON p.id = i.product_id
WHERE MONTH(p.launch_date) IN (6, 10, 12)
