SELECT id, supplier_id, stock_qty
FROM products
WHERE stock_qty < 318
EXCEPT
SELECT id, supplier_id, stock_qty
FROM products
WHERE cost >= 410;
