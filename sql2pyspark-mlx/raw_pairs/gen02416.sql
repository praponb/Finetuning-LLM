SELECT city, opened_date, sqft
FROM stores
WHERE region IN ('East', 'Central') AND sqft != 5238
