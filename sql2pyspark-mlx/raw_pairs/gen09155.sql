SELECT * FROM (SELECT brand, category, price FROM products)
PIVOT (MAX(price) FOR category IN ('Home', 'Grocery', 'Sports'))
