SELECT product_id, MAX(units) - MIN(units) AS units_range, MIN(units) AS min_units FROM store_sales GROUP BY product_id HAVING MIN(units) > 31;
