SELECT MAX(units) max_units, MAX(units) - MIN(units) units_range
FROM store_sales
WHERE units <= 34
ORDER BY units_range DESC;
