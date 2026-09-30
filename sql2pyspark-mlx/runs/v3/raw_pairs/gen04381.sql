SELECT product_id, MAX(revenue) AS max_revenue, MAX(units) - MIN(units) AS units_range FROM store_sales GROUP BY product_id ORDER BY max_revenue, product_id LIMIT 20
