SELECT id, product_id FROM store_sales WHERE MONTH(sale_date) IN (2, 7, 11) AND product_id = 6 AND units >= 30 ORDER BY product_id, id DESC;
