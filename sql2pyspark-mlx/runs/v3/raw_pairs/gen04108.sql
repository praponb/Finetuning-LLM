SELECT id, product_id, store_id, units, RANK() OVER (PARTITION BY product_id, store_id ORDER BY units DESC) AS rnk FROM store_sales;
