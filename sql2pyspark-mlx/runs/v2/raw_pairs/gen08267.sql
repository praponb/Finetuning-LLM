SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
