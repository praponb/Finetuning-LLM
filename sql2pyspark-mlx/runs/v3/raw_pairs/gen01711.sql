SELECT * FROM (SELECT id, store_id, revenue, RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) pos FROM store_sales) ranked WHERE pos <= 2;
