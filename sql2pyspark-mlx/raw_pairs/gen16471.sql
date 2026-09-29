SELECT AVG(units) AS avg_units, SUM(units) AS total_units
FROM store_sales
ORDER BY total_units DESC;
