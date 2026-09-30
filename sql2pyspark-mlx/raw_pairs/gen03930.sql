SELECT stock_qty FROM products WHERE product_name IS NOT NULL UNION SELECT stock_qty FROM products WHERE stock_qty BETWEEN 115 AND 255 ORDER BY stock_qty
