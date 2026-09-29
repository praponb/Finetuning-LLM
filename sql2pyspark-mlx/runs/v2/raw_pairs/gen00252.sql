select a.id, a.store_id, a.quantity, a.last_restock from inventory a where a.quantity = (select min(b.quantity) from inventory b where b.store_id = a.store_id) order by a.quantity desc;
