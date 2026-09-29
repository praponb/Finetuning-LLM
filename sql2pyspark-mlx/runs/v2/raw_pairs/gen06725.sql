SELECT product_id, store_id, MAX(units) AS max_units, MAX(revenue) - MIN(revenue) AS revenue_range FROM store_sales GROUP BY product_id, store_id
