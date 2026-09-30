SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, product_id, ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
WHERE product_id IN (2, 5)
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM'), product_id
ORDER BY sale_month;
