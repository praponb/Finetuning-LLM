SELECT id, launch_date, cost
FROM products
WHERE UPPER(TRIM(product_name)) = 'COFFEE MAKER'
ORDER BY cost, launch_date
