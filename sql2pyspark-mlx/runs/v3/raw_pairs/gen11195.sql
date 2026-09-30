SELECT product_name, brand
FROM products
WHERE launch_date BETWEEN '2023-04-01' AND '2024-05-01'
EXCEPT
SELECT product_name, brand
FROM products
WHERE category != 'Sports';
