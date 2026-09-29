SELECT launch_date, stock_qty FROM products WHERE price <= 1160 UNION ALL SELECT launch_date, stock_qty FROM products WHERE SIZE(tags) > 1;
