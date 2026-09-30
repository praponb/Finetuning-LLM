SELECT p.id, p.category, p.stock_qty FROM products p WHERE p.stock_qty > (SELECT AVG(p2.stock_qty) FROM products p2 WHERE p2.category = p.category) ORDER BY p.stock_qty DESC, p.id DESC
