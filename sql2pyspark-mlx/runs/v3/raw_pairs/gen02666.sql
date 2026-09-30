SELECT ROUND(STDDEV(revenue), 2) stddev_revenue, AVG(revenue) avg_revenue
FROM store_sales
WHERE YEAR(sale_date) >= 2024
