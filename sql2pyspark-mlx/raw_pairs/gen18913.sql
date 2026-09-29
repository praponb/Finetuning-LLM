SELECT id, product_name, tags, price + stock_qty AS price_plus_stock_qty FROM products WHERE category != 'Sports' AND (price BETWEEN 730 AND 1200 OR SIZE(tags) >= 2);
