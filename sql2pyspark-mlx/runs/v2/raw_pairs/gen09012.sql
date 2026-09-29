SELECT * FROM (SELECT brand, category, cost FROM products)
PIVOT (MAX(cost) FOR category IN ('Sports', 'Electronics', 'Grocery', 'Books'))
