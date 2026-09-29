SELECT *
FROM products
WHERE LOWER(product_name) LIKE '%esk%'
ORDER BY launch_date ASC, brand ASC;
