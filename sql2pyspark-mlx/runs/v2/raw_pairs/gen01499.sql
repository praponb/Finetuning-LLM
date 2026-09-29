SELECT * FROM products WHERE product_name NOT LIKE 'D%' AND supplier_id IS NULL AND stock_qty <= 243 ORDER BY id DESC LIMIT 3
