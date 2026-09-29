SELECT * FROM (SELECT supplier_id, category, price FROM products)
PIVOT (MAX(price) FOR category IN ('Grocery', 'Sports', 'Home'))
ORDER BY supplier_id
