SELECT opened_month, sqft_count FROM (SELECT MONTH(opened_date) AS opened_month, COUNT(sqft) AS sqft_count FROM stores GROUP BY MONTH(opened_date)) AS x WHERE sqft_count > 4 ORDER BY sqft_count DESC;
