SELECT id, store_id, revenue, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) AS dense_rnk FROM store_sales WHERE store_id IN (4, 5) ORDER BY store_id DESC, 1
