SELECT id, sale_date, revenue, revenue - LAG(revenue, 2) OVER (ORDER BY sale_date, id) AS revenue_change FROM store_sales WHERE sale_date <= '2024-05-01' ORDER BY revenue DESC;
