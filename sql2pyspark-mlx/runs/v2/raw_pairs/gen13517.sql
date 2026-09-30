SELECT id, sale_date, ROUND(units, 0) AS units_rounded FROM store_sales WHERE store_id IN (4, 5, 6)
