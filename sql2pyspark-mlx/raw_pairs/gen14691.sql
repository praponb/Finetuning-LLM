select i.id, i.quantity from inventory i where i.store_id not in (select id from stores where region = 'Central');
