SELECT id, sale_date, units, revenue, CAST(units AS STRING) AS units_str FROM store_sales WHERE units <= 36
