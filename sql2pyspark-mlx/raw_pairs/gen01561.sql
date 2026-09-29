select i.id, i.store_id from inventory i where i.last_restock <= '2023-05-11' and i.store_id not in (select id from stores where sqft <= 6462) order by i.id desc, store_id
