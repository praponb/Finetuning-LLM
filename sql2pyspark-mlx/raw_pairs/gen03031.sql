SELECT id, store_id, units, RANK() OVER (PARTITION BY store_id ORDER BY units) AS rnk FROM store_sales WHERE units = 27
