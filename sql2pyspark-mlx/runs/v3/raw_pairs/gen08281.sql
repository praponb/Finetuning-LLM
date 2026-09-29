SELECT
    i.id,
    i.store_id,
    i.product_id,
    p.stock_qty,
    p.product_name,
    s.opened_date
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
INNER JOIN stores s ON i.store_id = s.id
