SELECT location, 'department' AS source FROM departments UNION SELECT city AS location, 'store' AS source FROM stores WHERE sqft BETWEEN 9202 AND 12108 ORDER BY source
