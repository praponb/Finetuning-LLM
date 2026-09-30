SELECT id, sale_date, units * revenue AS revenue_per_unit FROM store_sales WHERE units BETWEEN 37 AND 39 AND sale_date <= '2025-05-01' ORDER BY revenue_per_unit DESC, id;
