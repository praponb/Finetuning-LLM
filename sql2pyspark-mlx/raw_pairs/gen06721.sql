SELECT id, price, supplier_id, stock_qty FROM products WHERE price > (SELECT AVG(price) FROM products WHERE ARRAY_CONTAINS(tags, 'clearance')) ORDER BY stock_qty;
