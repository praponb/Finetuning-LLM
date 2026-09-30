SELECT id, units, store_id
FROM store_sales
WHERE units >= (SELECT AVG(units) FROM store_sales WHERE revenue <= 3700)
ORDER BY id DESC
