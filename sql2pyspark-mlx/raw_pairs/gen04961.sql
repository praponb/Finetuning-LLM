SELECT store_id, COUNT(DISTINCT units) AS unique_products, ROUND(STDDEV(revenue), 2) AS stddev_revenue FROM store_sales GROUP BY store_id HAVING stddev_revenue < 53;
