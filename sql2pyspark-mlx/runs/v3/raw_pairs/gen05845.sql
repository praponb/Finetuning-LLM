SELECT id, category, price, stock_qty, CONCAT_WS('/', category, product_name) AS category_product_name_key FROM products
