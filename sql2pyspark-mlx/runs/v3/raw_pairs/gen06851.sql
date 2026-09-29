select ss.revenue, ss.store_id, s.sqft, s.store_name from store_sales ss join stores s on ss.store_id = s.id
