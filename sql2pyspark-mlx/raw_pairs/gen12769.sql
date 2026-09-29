SELECT QUARTER(launch_date) AS launch_quarter, brand, COUNT(*) AS num_products
FROM products
GROUP BY 1, 2
HAVING num_products > 5
ORDER BY brand DESC, num_products DESC
