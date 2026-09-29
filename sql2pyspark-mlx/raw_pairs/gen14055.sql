SELECT price, LEAST(stock_qty, price) AS least_stock_qty_price, LOWER(product_name) AS product_name_lower FROM products WHERE price < 1160;
