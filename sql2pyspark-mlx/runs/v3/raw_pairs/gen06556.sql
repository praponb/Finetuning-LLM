SELECT * FROM (SELECT category, brand, stock_qty FROM products WHERE supplier_id IN (4, 14, 17, 37)) PIVOT (COUNT(stock_qty) FOR brand IN ('Umbrella', 'Globex'))
