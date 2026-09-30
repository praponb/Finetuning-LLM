SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, product_id, ROUND(STDDEV(units), 2) AS stddev_units
FROM store_sales
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM'), product_id
ORDER BY sale_month, product_id
