select quantity, store_id from inventory where quantity is not null union select quantity, store_id from inventory where store_id in (2, 4, 5);
