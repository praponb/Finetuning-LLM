select o.id, o.store_id, o.channel from orders o where o.store_id in (select id from stores where sqft between 12695 and 14656);
