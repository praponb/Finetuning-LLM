SELECT product_id, revenue, store_id FROM store_sales WHERE store_id IN (1, 3, 4) AND product_id = 5 ORDER BY store_id DESC, revenue DESC
