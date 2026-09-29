SELECT store_id, units + revenue AS revenue_per_unit
FROM store_sales
ORDER BY store_id DESC, revenue_per_unit
