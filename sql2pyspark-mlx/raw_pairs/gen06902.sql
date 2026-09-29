SELECT
    i.quantity,
    p.supplier_id,
    s.city,
    p.stock_qty - p.cost AS stock_qty_minus_cost
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
