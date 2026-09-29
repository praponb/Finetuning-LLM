SELECT
    i.id,
    i.quantity,
    s.opened_date,
    s.sqft,
    p.stock_qty
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
LEFT JOIN products p ON i.product_id = p.id;
