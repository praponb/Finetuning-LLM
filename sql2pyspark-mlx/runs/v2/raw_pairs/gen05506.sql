SELECT product_id, store_id, ROUND(STDDEV(units), 2) AS stddev_units, MIN(sale_date) AS first_sale_date FROM store_sales GROUP BY product_id, store_id;
