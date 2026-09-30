SELECT id, revenue FROM store_sales WHERE revenue = (SELECT MIN(revenue) FROM store_sales) ORDER BY revenue DESC
