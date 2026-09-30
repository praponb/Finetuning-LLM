SELECT launch_date FROM products WHERE stock_qty <= 267 UNION ALL SELECT launch_date FROM products WHERE cost BETWEEN 660 AND 720 ORDER BY launch_date;
