select i.id, i.last_restock, i.quantity from inventory i where i.store_id in (select id from stores where region <> 'East')
