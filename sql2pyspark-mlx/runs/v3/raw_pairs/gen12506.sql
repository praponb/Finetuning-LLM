SELECT id, product_id, store_id, quantity, CEIL(quantity) AS quantity_whole, ROUND(quantity, 1) AS quantity_rounded FROM inventory ORDER BY product_id DESC, quantity_rounded DESC;
