SELECT YEAR(launch_date) AS launch_year, brand, COUNT(*) AS row_count
FROM products
GROUP BY YEAR(launch_date), brand;
