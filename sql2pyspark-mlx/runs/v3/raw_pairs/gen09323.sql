SELECT MAX(units) - MIN(units) units_range FROM store_sales WHERE product_id IN (7, 8, 9) AND (revenue > 3400 OR revenue >= 2000) ORDER BY units_range DESC;
