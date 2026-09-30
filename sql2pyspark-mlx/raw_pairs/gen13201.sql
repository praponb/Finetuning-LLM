select s.id, s.store_name from stores s where s.sqft = 12044 and exists (select 1 from inventory i where i.store_id = s.id and i.quantity < 83)
