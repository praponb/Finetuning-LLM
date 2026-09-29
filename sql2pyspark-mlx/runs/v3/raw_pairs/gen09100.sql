SELECT YEAR(opened_date) AS opened_year, COUNT(*) AS total_count FROM stores WHERE city != 'Berlin' AND region IN ('West', 'South', 'North') GROUP BY 1 HAVING COUNT(*) > 1
