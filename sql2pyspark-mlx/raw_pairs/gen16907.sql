select store_id from inventory where product_id in (2, 4, 6) union select store_id from inventory where product_id in (2, 7)
