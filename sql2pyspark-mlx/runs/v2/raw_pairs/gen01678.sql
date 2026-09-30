SELECT id, store_id, COALESCE(quantity, 0) AS quantity_filled
FROM inventory
ORDER BY store_id DESC, quantity_filled DESC;
