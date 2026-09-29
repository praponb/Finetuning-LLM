SELECT sale_date, revenue, store_id, revenue + units AS revenue_per_unit FROM store_sales WHERE sale_date >= '2024-10-06' AND (revenue > 2700 OR units BETWEEN 39 AND 40)
