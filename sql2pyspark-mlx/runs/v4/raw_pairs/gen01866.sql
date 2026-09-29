SELECT id, product_id, sale_date, units, LAG(units, 2) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_units FROM store_sales WHERE product_id IN (2, 7, 9)
