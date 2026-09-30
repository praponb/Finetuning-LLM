SELECT MIN(revenue) AS min_revenue, MAX(units) - MIN(units) AS units_range FROM store_sales WHERE MONTH(sale_date) IN (5, 6, 10)
