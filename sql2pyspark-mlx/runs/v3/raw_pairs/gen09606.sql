select i.id, i.store_id, i.quantity, i.last_restock from inventory i where i.product_id in (select id from products where category = 'Sports') order by last_restock
