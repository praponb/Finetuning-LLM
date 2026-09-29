SELECT MONTH(sale_date) AS sale_month, ROUND(STDDEV(units), 2) AS stddev_units, AVG(units) AS avg_units, MAX(units) - MIN(units) AS units_range FROM store_sales GROUP BY MONTH(sale_date)
