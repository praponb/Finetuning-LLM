SELECT stock_qty
FROM products
WHERE product_name LIKE '%Backpac%'
UNION
SELECT stock_qty
FROM products
WHERE brand = 'Initech';
