SELECT *, CAST(quantity AS DOUBLE) AS quantity_num FROM order_items WHERE product_id IS NULL AND unit_price >= 310 ORDER BY order_id DESC, id ASC LIMIT 3;
