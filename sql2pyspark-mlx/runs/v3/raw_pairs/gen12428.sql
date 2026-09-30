select i.id, i.product_id, i.quantity, i.store_id from inventory i where i.quantity = (select min(i2.quantity) from inventory i2 where i2.product_id = i.product_id)
