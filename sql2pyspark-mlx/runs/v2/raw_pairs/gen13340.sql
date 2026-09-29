select i.id, i.last_restock, i.quantity, i.store_id from inventory i where i.product_id not in (select id from products where category not in ('Fashion', 'Books'));
