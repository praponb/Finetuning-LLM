SELECT CASE WHEN sqft >= 17568 THEN 'high' WHEN sqft >= 4794 THEN 'mid' ELSE 'low' END AS sqft_bucket, city, COUNT(DISTINCT sqft) AS unique_regions
FROM stores
WHERE opened_date < '2023-09-01' OR opened_date >= '2023-04-01'
GROUP BY sqft_bucket, city
HAVING COUNT(DISTINCT sqft) >= 1
