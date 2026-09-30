SELECT id, sqft
FROM stores
WHERE opened_date <= '2023-07-01'
INTERSECT
SELECT id, sqft
FROM stores
WHERE sqft <> 12990;
