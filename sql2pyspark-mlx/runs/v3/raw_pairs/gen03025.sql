SELECT store_id, sale_date, revenue + units AS revenue_per_unit FROM store_sales WHERE sale_date BETWEEN '2024-08-01' AND '2025-04-01' ORDER BY 3 DESC
