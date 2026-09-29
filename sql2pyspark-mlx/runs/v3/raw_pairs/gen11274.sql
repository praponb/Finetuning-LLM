SELECT id, units, units + revenue AS revenue_per_unit, ROUND(units * 100, 2) AS adjusted_units FROM store_sales WHERE store_id IN (4, 5, 6) ORDER BY id DESC;
