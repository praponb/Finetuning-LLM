SELECT id, product_id, sale_date FROM (SELECT id, product_id, sale_date, RANK() OVER (PARTITION BY product_id ORDER BY sale_date) AS pos FROM store_sales) AS ranked WHERE pos <= 3;
