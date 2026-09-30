SELECT id, store_id, revenue, RANK() OVER (PARTITION BY store_id ORDER BY revenue) AS rnk FROM store_sales WHERE product_id IN (7, 8) ORDER BY id DESC
