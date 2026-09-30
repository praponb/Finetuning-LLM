SELECT * FROM (SELECT id, product_id, units, store_id, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS rnk FROM store_sales) AS ranked WHERE rnk <= 2 ORDER BY product_id DESC;
