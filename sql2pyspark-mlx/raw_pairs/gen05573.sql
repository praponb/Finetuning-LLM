SELECT * FROM (SELECT category, brand, stock_qty FROM products WHERE brand IN ('Stark', 'Acme')) PIVOT (COUNT(stock_qty) FOR brand IN ('Globex', 'Umbrella', 'Acme', 'Initech'));
