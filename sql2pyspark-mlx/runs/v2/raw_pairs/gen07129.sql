SELECT id, price, product_name, tags, UPPER(brand) AS brand_upper FROM products WHERE cost >= 470 AND stock_qty <= 379 ORDER BY product_name DESC;
