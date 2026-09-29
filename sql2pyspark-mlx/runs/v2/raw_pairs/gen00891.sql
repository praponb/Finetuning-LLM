select id, product_id from inventory where month(last_restock) in (3, 6, 12)
