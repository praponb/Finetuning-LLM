SELECT tags, stock_qty + cost AS stock_qty_plus_cost FROM products WHERE category IN ('Books', 'Sports', 'Fashion') ORDER BY stock_qty_plus_cost DESC;
