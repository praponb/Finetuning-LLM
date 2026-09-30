SELECT id, sale_date, store_id, units, ROW_NUMBER() OVER (PARTITION BY MONTH(sale_date), store_id ORDER BY units, id) AS units_row FROM store_sales WHERE product_id = 4
