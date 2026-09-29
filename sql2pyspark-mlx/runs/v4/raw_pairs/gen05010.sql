select ss.id, ss.units, s.opened_date, s.city, s.sqft from store_sales ss left join stores s on ss.store_id = s.id where ss.store_id in (4, 6)
