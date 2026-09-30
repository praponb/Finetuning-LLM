SELECT id, price, brand
FROM products
WHERE category NOT IN ('Electronics', 'Books', 'Sports') AND launch_date < '2023-08-01'
ORDER BY price DESC
