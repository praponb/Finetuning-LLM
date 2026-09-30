SELECT store_id, ROUND(STDDEV(revenue), 2) AS stddev_revenue FROM store_sales WHERE units < 29 AND MONTH(sale_date) IN (1, 10, 11) GROUP BY store_id;
