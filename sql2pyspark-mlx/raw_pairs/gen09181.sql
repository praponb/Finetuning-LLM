SELECT id, store_id, units, revenue FROM (SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY units) AS pos FROM store_sales WHERE product_id = 9) AS t WHERE pos <= 2
