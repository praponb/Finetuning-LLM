SELECT id, launch_date, RANK() OVER (PARTITION BY MONTH(launch_date) ORDER BY launch_date) AS rnk
FROM products;
