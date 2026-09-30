SELECT id, cost, tags FROM products WHERE NOT (stock_qty BETWEEN 169 AND 356 AND launch_date < '2023-11-01');
