SELECT id, units
FROM store_sales
WHERE units <= (SELECT MIN(units) FROM store_sales)
ORDER BY 1, units;
