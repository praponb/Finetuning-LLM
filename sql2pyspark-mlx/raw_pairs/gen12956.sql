SELECT id, store_id FROM inventory EXCEPT ALL SELECT id, store_id FROM inventory WHERE YEAR(last_restock) >= 2025;
