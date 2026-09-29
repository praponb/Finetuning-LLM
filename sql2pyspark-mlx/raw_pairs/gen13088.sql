select stores.id, stores.region from stores where exists (select 1 from inventory where inventory.store_id = stores.id and inventory.last_restock > '2025-04-01') order by id;
