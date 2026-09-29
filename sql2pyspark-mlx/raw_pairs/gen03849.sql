SELECT * FROM (SELECT QUARTER(opened_date) AS opened_quarter, COUNT(sqft) AS sqft_count FROM stores GROUP BY QUARTER(opened_date)) AS t WHERE sqft_count > 5 ORDER BY 1 LIMIT 20;
