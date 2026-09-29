SELECT a.id, a.category, a.stock_qty FROM products a WHERE a.stock_qty = (SELECT MAX(b.stock_qty) FROM products b WHERE b.category = a.category);
