SELECT * FROM (SELECT category, brand, stock_qty FROM products)
PIVOT (AVG(stock_qty) FOR brand IN ('Umbrella', 'Globex'))
