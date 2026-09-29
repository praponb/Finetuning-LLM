SELECT id, store_name, ROUND(sqft * 0.85, 2) AS adjusted_sqft, CASE WHEN region = 'Central' THEN 1 ELSE 0 END AS is_central FROM stores WHERE YEAR(opened_date) >= 2025
