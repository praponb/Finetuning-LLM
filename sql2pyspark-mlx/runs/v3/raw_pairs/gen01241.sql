SELECT id, launch_date, price, supplier_id FROM products WHERE LENGTH(product_name) > 11 AND stock_qty >= 188 AND SIZE(tags) >= 2;
