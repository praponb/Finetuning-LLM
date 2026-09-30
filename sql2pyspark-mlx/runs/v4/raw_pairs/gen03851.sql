SELECT *, LOWER(region) AS region_lower, LENGTH(region) AS region_length
FROM stores
WHERE opened_date BETWEEN '2023-07-15' AND '2025-10-05'
