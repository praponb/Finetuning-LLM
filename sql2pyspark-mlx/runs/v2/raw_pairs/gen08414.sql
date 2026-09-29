SELECT id, sale_date, units, units - LAG(units, 1, 0) OVER (ORDER BY sale_date, id) AS units_change FROM store_sales;
