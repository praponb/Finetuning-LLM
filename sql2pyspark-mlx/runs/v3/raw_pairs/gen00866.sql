SELECT QUARTER(sale_date) AS sale_quarter, ROUND(STDDEV(units), 2) AS stddev_units FROM store_sales WHERE revenue >= 1400 GROUP BY QUARTER(sale_date) ORDER BY stddev_units, sale_quarter LIMIT 1;
