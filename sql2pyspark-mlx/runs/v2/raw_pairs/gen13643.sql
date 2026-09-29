SELECT city
FROM suppliers
INTERSECT
SELECT city
FROM stores
WHERE sqft > 7603
ORDER BY city
