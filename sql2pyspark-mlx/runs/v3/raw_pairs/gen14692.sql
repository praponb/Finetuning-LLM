SELECT city, region, ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
WHERE sqft <> 14556 AND sqft <> 9090 AND opened_date BETWEEN '2023-11-15' AND '2025-10-01'
GROUP BY city, region
HAVING COUNT(DISTINCT sqft) >= 5;
