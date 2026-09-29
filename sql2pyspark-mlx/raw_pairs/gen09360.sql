SELECT id, store_id, quantity FROM inventory QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) = 1;
