SELECT *, stock_qty + cost AS stock_qty_plus_cost
FROM products
WHERE brand IN ('Initech', 'Umbrella', 'Stark');
