SELECT * FROM (SELECT category, COUNT(*) row_count, MIN(launch_date) first_launch_date FROM products GROUP BY category) x WHERE row_count >= 8 ORDER BY 1 DESC, row_count ASC
