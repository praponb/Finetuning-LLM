SELECT id, launch_date, RANK() OVER (ORDER BY launch_date) AS launch_date_rank
FROM products;
