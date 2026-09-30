SELECT * FROM (SELECT category, brand, stock_qty FROM products WHERE stock_qty > 210) PIVOT (SUM(stock_qty) FOR brand IN ('Acme', 'Umbrella', 'Stark')) ORDER BY category
