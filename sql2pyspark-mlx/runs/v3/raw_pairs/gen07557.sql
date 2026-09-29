SELECT product_id, ROUND(STDDEV(units), 2) AS stddev_units, COUNT(DISTINCT revenue) AS unique_products FROM store_sales GROUP BY product_id
