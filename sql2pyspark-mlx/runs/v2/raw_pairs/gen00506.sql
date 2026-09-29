SELECT units, sale_date, revenue, units + revenue AS revenue_per_unit FROM store_sales WHERE store_id IN (5, 6) ORDER BY revenue, units DESC;
