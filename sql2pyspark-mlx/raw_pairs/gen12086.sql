SELECT MONTH(opened_date) AS opened_month, ROUND(AVG(sqft), 2) AS avg_sqft FROM stores WHERE sqft >= 6790 AND city IN ('Bangkok', 'New York', 'Chiang Mai') GROUP BY MONTH(opened_date);
