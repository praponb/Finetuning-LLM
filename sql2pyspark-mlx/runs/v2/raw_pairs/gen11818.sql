SELECT id, store_id, units, NTILE(4) OVER (PARTITION BY store_id ORDER BY units DESC, id) AS quartile FROM store_sales WHERE units = 18 ORDER BY store_id DESC;
