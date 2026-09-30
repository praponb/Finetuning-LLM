SELECT * FROM (SELECT brand, category, cost FROM products)
PIVOT (AVG(cost) FOR category IN ('Books', 'Electronics', 'Sports', 'Fashion'))
ORDER BY brand
