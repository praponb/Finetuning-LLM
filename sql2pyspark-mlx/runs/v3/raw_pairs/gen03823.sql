SELECT * FROM (SELECT city, region, sqft FROM stores WHERE opened_date > '2024-10-27')
PIVOT (COUNT(sqft) FOR region IN ('East', 'West', 'North', 'Central'))
ORDER BY city
