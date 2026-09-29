SELECT id, revenue, units, sale_date, CAST(revenue AS INT) AS revenue_int, COALESCE(units, 0) AS units_filled FROM store_sales WHERE revenue IS NOT NULL ORDER BY revenue DESC, id LIMIT 20;
