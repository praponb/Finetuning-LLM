SELECT *, COUNT(revenue) OVER (PARTITION BY product_id) product_id_count_revenue, LAG(units, 1, 0) OVER (PARTITION BY product_id ORDER BY revenue, id) prev_units FROM store_sales
