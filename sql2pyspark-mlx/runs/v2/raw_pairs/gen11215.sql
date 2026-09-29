SELECT * FROM (SELECT brand, category, price FROM products)
PIVOT (MAX(price) FOR category IN ('Fashion', 'Sports', 'Electronics'))
ORDER BY brand
