SELECT id, store_id, revenue, product_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS pos FROM store_sales) AS ranked WHERE pos <= 2
