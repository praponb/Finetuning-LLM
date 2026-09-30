SELECT id, opened_date, sqft
FROM stores
WHERE opened_date = (SELECT MAX(opened_date) FROM stores)
ORDER BY sqft DESC, id
LIMIT 5;
