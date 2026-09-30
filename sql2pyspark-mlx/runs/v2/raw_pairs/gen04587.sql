SELECT store_id, MONTH(sale_date) AS sale_month, MAX(sale_date) AS last_sale_date, ROUND(STDDEV(revenue), 2) AS stddev_revenue FROM store_sales GROUP BY store_id, MONTH(sale_date)
