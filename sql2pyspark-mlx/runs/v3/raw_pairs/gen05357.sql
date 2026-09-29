SELECT id, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC, id) revenue_row FROM store_sales;
