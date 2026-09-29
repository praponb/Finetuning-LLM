SELECT id, product_id, quantity FROM (SELECT id, product_id, quantity, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS pos FROM inventory) AS t WHERE pos <= 5 ORDER BY 2;
