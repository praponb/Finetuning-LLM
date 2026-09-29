select s.id, s.region, s.sqft, s.opened_date from stores s where exists (select 1 from store_sales ss where ss.store_id = s.id)
