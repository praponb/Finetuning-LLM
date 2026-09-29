SELECT product_id, store_id, COUNT(quantity) AS quantity_count, COUNT(DISTINCT quantity) AS unique_stores FROM inventory WHERE store_id IN (1, 2, 5, 6) AND store_id = 4 GROUP BY product_id, store_id;
