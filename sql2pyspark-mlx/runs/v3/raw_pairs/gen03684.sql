SELECT city, SUM(CASE WHEN region = 'North' THEN sqft ELSE 0 END) north_sqft FROM stores WHERE YEAR(opened_date) >= 2023 AND city = 'Singapore' GROUP BY city ORDER BY city
