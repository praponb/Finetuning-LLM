select id, store_id, product_id from inventory where quantity < 90 union all select id, store_id, product_id from inventory where quantity != 91
