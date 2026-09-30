SELECT
    i.quantity,
    i.product_id,
    s.sqft,
    s.store_name,
    p.stock_qty,
    p.supplier_id
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
FULL OUTER JOIN products p ON p.id = i.product_id
ORDER BY p.stock_qty DESC;
