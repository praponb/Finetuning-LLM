SELECT MIN(sale_date) AS min_sale_date, MAX(units) - MIN(units) AS units_range FROM store_sales WHERE store_id IN (2, 3, 5, 6) AND store_id = 5 AND MONTH(sale_date) IN (1, 2, 7)
