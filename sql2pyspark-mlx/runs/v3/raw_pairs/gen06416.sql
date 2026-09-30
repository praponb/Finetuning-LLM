SELECT QUARTER(sale_date) sale_quarter, ROUND(STDDEV(revenue), 2) stddev_revenue, ROUND(AVG(revenue), 2) avg_revenue
FROM store_sales
GROUP BY QUARTER(sale_date)
