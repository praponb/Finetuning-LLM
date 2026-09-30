SELECT id, store_id, revenue, product_id, units + revenue AS revenue_per_unit FROM store_sales WHERE sale_date > '2023-09-18' OR revenue BETWEEN 1200 AND 3300 ORDER BY 2 DESC
