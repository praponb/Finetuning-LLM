select s.id, s.store_name, s.sqft from stores s where not exists (select 1 from orders o where o.store_id = s.id and o.customer_id in (8, 10, 25, 37))
