SELECT id, product_name, stock_qty
FROM products
WHERE product_name IS NOT NULL AND supplier_id = 26
ORDER BY stock_qty DESC, id
LIMIT 3;
