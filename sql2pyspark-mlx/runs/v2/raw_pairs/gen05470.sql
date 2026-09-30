SELECT id, quantity, store_id, LAST_VALUE(store_id) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_store_id FROM inventory;
