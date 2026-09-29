SELECT region, MAX(sqft) AS max_sqft
FROM stores
WHERE opened_date < '2023-04-01'
GROUP BY region;
