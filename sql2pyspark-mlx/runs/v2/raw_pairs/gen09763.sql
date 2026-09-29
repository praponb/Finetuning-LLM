SELECT ROUND(STDDEV(revenue), 2) stddev_revenue, AVG(units) avg_units FROM store_sales WHERE YEAR(sale_date) >= 2023 ORDER BY stddev_revenue DESC;
