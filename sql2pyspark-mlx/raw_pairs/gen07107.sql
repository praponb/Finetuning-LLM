SELECT id, units, product_id, revenue FROM store_sales WHERE units >= (SELECT MAX(units) FROM store_sales) ORDER BY product_id DESC;
