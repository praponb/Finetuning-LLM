SELECT *, ROUND(price, 0) AS price_rounded
FROM products
WHERE category = 'Grocery'
ORDER BY launch_date
