SELECT city, region, COUNT(DISTINCT sqft) AS unique_citys FROM stores WHERE opened_date <= '2023-09-01' AND (sqft >= 10119 OR YEAR(opened_date) = 2024) GROUP BY city, region ORDER BY city;
