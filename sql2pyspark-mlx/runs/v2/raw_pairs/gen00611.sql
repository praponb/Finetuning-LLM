select i.id, i.product_id, i.quantity, i.last_restock from inventory i where i.quantity > (select avg(i2.quantity) from inventory i2 where i2.product_id = i.product_id)
