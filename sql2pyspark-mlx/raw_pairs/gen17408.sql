SELECT * FROM (SELECT category, brand, stock_qty FROM products WHERE category IN ('Grocery', 'Fashion')) PIVOT (SUM(stock_qty) FOR brand IN ('Stark', 'Globex', 'Umbrella')) ORDER BY category;
