SELECT CASE WHEN region IN ('North', 'West') THEN 'core' ELSE 'other' END AS region_group, ROUND(AVG(sqft), 2) AS avg_sqft, MAX(opened_date) AS latest_opened_date
FROM stores
WHERE YEAR(opened_date) >= 2024
GROUP BY region_group;
