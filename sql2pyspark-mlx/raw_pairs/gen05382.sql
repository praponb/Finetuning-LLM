SELECT id, product_name, LEAST(price, stock_qty) AS least_price_stock_qty, SUBSTRING(category, 1, 2) AS category_prefix FROM products WHERE supplier_id IS NOT NULL AND product_name IS NOT NULL;
