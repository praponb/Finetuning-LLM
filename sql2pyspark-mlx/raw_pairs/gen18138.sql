SELECT s.rating, p.launch_date, p.cost + p.stock_qty cost_plus_stock_qty
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id;
