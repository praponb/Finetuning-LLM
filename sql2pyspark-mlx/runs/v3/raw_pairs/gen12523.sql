SELECT ROUND(STDDEV(revenue), 2) stddev_revenue FROM store_sales WHERE product_id IN (3, 4) AND MONTH(sale_date) IN (4, 7, 11);
