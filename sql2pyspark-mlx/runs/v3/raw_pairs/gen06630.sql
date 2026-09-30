select a.id, a.store_id, a.quantity, a.product_id from inventory a where a.quantity < (select avg(b.quantity) from inventory b where b.store_id = a.store_id)
