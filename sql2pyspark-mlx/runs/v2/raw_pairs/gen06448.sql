SELECT * FROM (SELECT id, store_id, quantity, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS pos FROM inventory) AS ranked WHERE pos <= 3;
