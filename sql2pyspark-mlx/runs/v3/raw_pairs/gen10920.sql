SELECT id, units, product_id, sale_date, ROUND(revenue, 1) AS revenue_rounded, revenue + units AS revenue_per_unit FROM store_sales WHERE sale_date BETWEEN '2023-08-01' AND '2023-09-18'
