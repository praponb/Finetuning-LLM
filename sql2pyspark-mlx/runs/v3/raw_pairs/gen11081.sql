SELECT * FROM (SELECT brand, category, cost FROM products)
PIVOT (COUNT(cost) FOR category IN ('Fashion', 'Sports', 'Electronics', 'Books'))
ORDER BY brand
