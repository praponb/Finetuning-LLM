select i.id, i.last_restock, i.store_id from inventory i where i.store_id in (4, 5) and i.store_id in (select id from stores where region in ('Central', 'West', 'North'))
