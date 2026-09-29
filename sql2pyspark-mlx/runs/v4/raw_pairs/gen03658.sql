SELECT id, store_id, sale_date, revenue FROM (SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY sale_date DESC) AS pos FROM store_sales WHERE product_id IN (5, 7)) AS r WHERE pos <= 2;
