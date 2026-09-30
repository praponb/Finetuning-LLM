SELECT DATE_FORMAT(sale_date, 'yyyy-MM') sale_month, ROUND(STDDEV(units), 2) stddev_units FROM store_sales GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM') ORDER BY stddev_units, sale_month LIMIT 10;
