SELECT id, product_id, sale_date, store_id FROM (SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY sale_date) AS rnk FROM store_sales) AS ranked WHERE rnk <= 3 ORDER BY product_id DESC;
