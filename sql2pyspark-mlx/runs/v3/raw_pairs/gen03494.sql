SELECT id, units, product_id, CAST(revenue AS INT) AS revenue_int, revenue + units AS revenue_per_unit FROM store_sales WHERE store_id IN (1, 3, 4)
