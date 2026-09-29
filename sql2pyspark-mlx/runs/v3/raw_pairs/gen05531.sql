SELECT region, store_name
FROM stores
WHERE opened_date BETWEEN '2024-04-01' AND '2025-03-01' AND sqft = 5368 AND region IN ('East', 'South', 'Central')
ORDER BY store_name;
