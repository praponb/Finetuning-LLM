SELECT location
FROM departments
INTERSECT
SELECT city
FROM stores
WHERE sqft > 5318;
