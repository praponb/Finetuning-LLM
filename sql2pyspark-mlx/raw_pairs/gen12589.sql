SELECT id, stock_qty, cost FROM products WHERE ARRAY_CONTAINS(tags, 'eco') ORDER BY stock_qty DESC, cost DESC, id ASC LIMIT 5
