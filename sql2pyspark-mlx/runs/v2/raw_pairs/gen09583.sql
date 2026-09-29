SELECT * FROM (SELECT brand, category, stock_qty FROM products WHERE brand = 'Umbrella') PIVOT (MAX(stock_qty) FOR category IN ('Grocery', 'Fashion', 'Electronics', 'Sports'))
