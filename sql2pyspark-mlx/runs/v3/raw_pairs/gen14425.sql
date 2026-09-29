SELECT * FROM (SELECT brand, category, cost FROM products)
PIVOT (AVG(cost) FOR category IN ('Sports', 'Books', 'Electronics'))
ORDER BY brand
