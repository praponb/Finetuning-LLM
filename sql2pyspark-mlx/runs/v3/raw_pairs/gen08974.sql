SELECT sale_date, units + revenue AS revenue_per_unit
FROM store_sales
WHERE store_id IN (1, 2, 3, 5) AND units <= 15
ORDER BY revenue_per_unit DESC
