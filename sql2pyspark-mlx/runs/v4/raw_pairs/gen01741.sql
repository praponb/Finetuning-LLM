SELECT id, units
FROM store_sales
WHERE units >= (SELECT AVG(units) FROM store_sales)
ORDER BY units, id
LIMIT 5;
