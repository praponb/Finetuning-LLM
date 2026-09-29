select s.id, s.sqft from stores s where exists (select 1 from inventory i where i.store_id = s.id and i.product_id = 3);
