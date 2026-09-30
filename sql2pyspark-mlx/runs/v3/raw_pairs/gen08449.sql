select ss.id, ss.store_id from store_sales ss where ss.store_id in (2, 3, 4, 6) and ss.store_id in (select id from stores where sqft <> 6911);
