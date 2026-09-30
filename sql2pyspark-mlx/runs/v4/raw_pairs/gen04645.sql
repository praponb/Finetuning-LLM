SELECT launch_date FROM products WHERE stock_qty > 159 INTERSECT SELECT launch_date FROM products WHERE category IN ('Books', 'Electronics');
