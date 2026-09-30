SELECT id, store_id, units, product_id FROM (SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY units DESC) row_num FROM store_sales) r WHERE row_num <= 5
