SELECT id, revenue, product_id FROM store_sales WHERE revenue >= (SELECT MAX(revenue) FROM store_sales)
