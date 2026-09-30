SELECT id, sqft, city FROM stores WHERE city != 'Chiang Mai' UNION ALL SELECT id, sqft, city FROM stores WHERE sqft > 15107 ORDER BY sqft
