SELECT *
FROM stores
WHERE region = 'North' AND opened_date > '2024-09-01'
ORDER BY sqft DESC, region, id
LIMIT 10;
