SELECT id, stock_qty FROM products WHERE launch_date > '2024-05-01' EXCEPT SELECT id, stock_qty FROM products WHERE price < 1030;
