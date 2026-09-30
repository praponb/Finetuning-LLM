SELECT id, store_id, revenue, product_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) pos FROM store_sales WHERE store_id IN (1, 4)) t WHERE pos <= 2
