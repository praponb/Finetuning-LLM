select ss.store_id, s.store_name, s.sqft from store_sales ss join stores s on ss.store_id = s.id
