SELECT MONTH(opened_date) AS opened_month, COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count, MIN(sqft) AS min_sqft FROM stores GROUP BY MONTH(opened_date) ORDER BY opened_month DESC
