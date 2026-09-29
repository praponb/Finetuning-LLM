SELECT product_id, YEAR(sale_date) sale_year, ROUND(STDDEV(units), 2) stddev_units, COUNT(units) units_count FROM store_sales GROUP BY product_id, YEAR(sale_date) HAVING stddev_units > 61
