select product_id, quantity from inventory where quantity <= 216 union select product_id, quantity from inventory where last_restock > '2024-02-24';
